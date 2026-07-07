import io
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Union

import requests

from pyveb.s3_client import s3Client
from pyveb.custom_decorators import retry
from pyveb.common import get_secret


@dataclass
class sharepointFile:
    name: str
    last_modified_date: Optional[datetime]
    creation_date: Optional[datetime]
    url: Optional[str]
    uri: Optional[str]
    version: Optional[str]
    relative_url: Optional[str]

    # Graph-specific fields
    id: str
    parent_path: Optional[str]
    download_url: Optional[str]
    size: Optional[int]


class sharepointClient:
    """
    Microsoft Graph based SharePoint client.
    """

    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
    TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    def __init__(
            self,
            site_path: str,
            drive_name: str,
            aws_secret_name: str,
            aws_secret_region: str = "eu-west-1"
    ) -> None:
        try:
            secret_details = json.loads(get_secret(aws_secret_name, aws_secret_region))
            self.tenant_id = secret_details['tenant_id']
            self.client_id = secret_details['client_id']
            self.client_secret = secret_details['client_secret']

        except KeyError:
            logging.error(f'Issue loading Tenant ID, Client ID and/or client Secret from aws secret {aws_secret_name}')
            sys.exit(1)

        self._access_token = None
        self._access_token_expires_at = 0

        self.site_id = self._get_site_id_by_path(site_path)
        self.drive_name = drive_name
        self.drive_id = self._get_drive_id_by_name(drive_name)
        self._drive_base_url = f'{self.GRAPH_BASE_URL}/drives/{self.drive_id}'

    def _get_access_token(self) -> str:
        """
        Client credentials flow for Microsoft Graph.
        """
        now = int(time.time())

        if self._access_token and now < self._access_token_expires_at - 60:
            return self._access_token

        token_url = self.TOKEN_URL_TEMPLATE.format(tenant_id=self.tenant_id)

        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        response = requests.post(token_url, data=payload, timeout=60)

        try:
            response.raise_for_status()
        except requests.HTTPError:
            logging.error(f"Failed to obtain Graph token: {response.status_code} - {response.text}")
            raise

        token_data = response.json()

        self._access_token = token_data["access_token"]
        self._access_token_expires_at = now + int(token_data.get("expires_in", 3599))

        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    def _graph_get(self, url: str, stream: bool = False) -> requests.Response:
        response = requests.get(url, headers=self._headers(), timeout=120, stream=stream)

        try:
            response.raise_for_status()
        except requests.HTTPError:
            logging.error(f"Graph GET failed: {response.status_code} - {response.text}")
            raise

        return response

    def _graph_put(self, url: str, body: Union[bytes, io.BytesIO]) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/octet-stream",
        }

        response = requests.put(url, headers=headers, data=body, timeout=300)

        try:
            response.raise_for_status()
        except requests.HTTPError:
            logging.error(f"Graph PUT failed: {response.status_code} - {response.text}")
            raise

        return response

    @staticmethod
    def _parse_graph_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        # Graph usually returns ISO strings like: 2026-07-06T10:30:00Z
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def parse_sharepoint_file_object(obj: dict) -> sharepointFile:
        """
        Parse a Microsoft Graph driveItem into the old sharepointFile shape.
        """
        return sharepointFile(
            name=obj.get("name"),
            last_modified_date=sharepointClient._parse_graph_datetime(obj.get("lastModifiedDateTime")),
            creation_date=sharepointClient._parse_graph_datetime(obj.get("createdDateTime")),
            url=obj.get("webUrl"),
            uri=obj.get("webUrl"),
            version=obj.get("eTag"),
            relative_url=obj.get("parentReference", {}).get("path"),
            id=obj["id"],
            parent_path=obj.get("parentReference", {}).get("path"),
            download_url=obj.get("@microsoft.graph.downloadUrl"),
            size=obj.get("size"),
        )

    def _get_site_id_by_path(self, site_url: str) -> str:
        url = f"{self.GRAPH_BASE_URL}/sites/{site_url}"
        response = self._graph_get(url)
        return response.json()["id"]

    def _get_drive_id_by_name(self, drive_name: str) -> str:
        url = f"{self.GRAPH_BASE_URL}/sites/{self.site_id}/drives"

        drives = []

        while url:
            response = self._graph_get(url)
            data = response.json()
            drives.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        matches = [
            drive for drive in drives
            if drive.get("name") == drive_name
        ]

        if not matches:
            found = [drive.get("name") for drive in drives]
            raise FileNotFoundError(
                f"Drive '{drive_name}' not found on site '{self.site_id}'. "
                f"Found drives: {found}"
            )

        if len(matches) > 1:
            raise ValueError(
                f"Multiple drives named '{drive_name}' found on site '{self.site_id}': "
                f"{[drive.get('id') for drive in matches]}"
            )

        return matches[0]["id"]

    def list_files(self, folder_prefix: str) -> List[sharepointFile]:
        """
        List files in a SharePoint folder using Microsoft Graph.

        Example:
            folder_prefix = "B&O Facturatie"

        Returns only files, not subfolders.
        """

        url = f"{self._drive_base_url}/root:/{folder_prefix}:/children"

        items = []

        while url:
            response = self._graph_get(url)
            data = response.json()

            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

        files = [
            self.parse_sharepoint_file_object(item)
            for item in items
            if "file" in item
        ]

        return files

    @retry(retries=3, error="Error download SharePoint file to S3")
    def download_to_s3(
        self,
        file: sharepointFile,
        s3_prefix: str,
        s3_bucket: str = "veb-data-pipelines",
        **kwargs,
    ) -> None:
        """
        Downloads a SharePoint file to S3.
        """

        # Download via driveItem content endpoint
        url = f"{self._drive_base_url}/items/{file.id}/content"
        response = self._graph_get(url, stream=True)

        bytes_file_obj = io.BytesIO(response.content)
        bytes_file_obj.seek(0)
        s3 = s3Client(s3_bucket)
        file_name = file.name.replace(" ", "_")
        if s3_prefix.endswith("/"):
            key = f'{s3_prefix}{file_name}'
        else:
            key = f'{s3_prefix}/{file_name}'
        s3.client.put_object(Body=bytes_file_obj, Bucket=s3_bucket, Key=key)
        logging.info(f"Wrote {file.name} to s3://{s3_bucket}/{key}")

    def upload_to_sharepoint(
        self,
        file,
        sharepoint_folder_prefix: str,
        file_name: str,
        file_extension: str,
        file_suffix_type: str = None,
        **kwargs,
    ) -> str:
        """
        Uploads a bytes object or local file to SharePoint using Microsoft Graph.

        For files up to 250 MB, Graph supports simple PUT upload.
        For larger files, this needs to be rewritten to use an upload session.

        Example:
            sharepoint_folder_prefix = "Gedeelde documenten/werkmap"
            file_name = "terra_extract"
            file_extension = "xlsx"

        Result:
            terra_extract.xlsx
        """
        valid_suffixes = ["current_date", "unix_timestamp", None]
        if file_suffix_type not in valid_suffixes:
            raise ValueError(f"Invalid file suffix provided in config. Accepted values are: {valid_suffixes}")

        target_suffix = None

        if file_suffix_type == "current_date":
            target_suffix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        elif file_suffix_type == "unix_timestamp":
            target_suffix = int(time.time())

        if target_suffix:
            target_name = f"{file_name}_{target_suffix}.{file_extension}"
        else:
            target_name = f"{file_name}.{file_extension}"

        url = f"{self._drive_base_url}/root:/{sharepoint_folder_prefix}/{target_name}:/content"

        body = self._coerce_file_to_bytes(file)

        # Simple upload limit is 250 MB
        if len(body) > 250 * 1024 * 1024:
            raise ValueError(
                "File is larger than 250 MB. Microsoft Graph simple upload only supports files up to 250 MB. "
                "Use an upload session for large files."
            )

        response = self._graph_put(url, body)
        data = response.json()

        logging.info(f"Uploaded file to SharePoint: {data.get('webUrl')}")

        return data.get("webUrl") or data.get("id")

    @staticmethod
    def _coerce_file_to_bytes(file) -> bytes:
        """
        Accepts:
            - bytes
            - BytesIO
            - file-like object
            - local file path string
        """
        if isinstance(file, bytes):
            return file

        if isinstance(file, io.BytesIO):
            file.seek(0)
            return file.read()

        if hasattr(file, "read"):
            return file.read()

        if isinstance(file, str):
            with open(file, "rb") as f:
                return f.read()

        raise TypeError(
            "Unsupported file type. Expected bytes, BytesIO, file-like object, or local file path string."
        )