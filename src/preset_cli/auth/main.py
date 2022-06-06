"""
Mechanisms for authentication and authorization.
"""

from typing import Dict, Optional

import requests
from yarl import URL


class Auth:  # pylint: disable=too-few-public-methods
    """
    An authentication/authorization mechanism.
    """

    def __init__(self):
        self.session = requests.Session()
        self.headers = {}

    def get_session(self) -> requests.Session:
        """
        Return a session.
        """
        return self.session

    def get_headers(self) -> Dict[str, str]:  # pylint: disable=no-self-use
        """
        Return headers for auth.
        """
        return self.headers


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via username/password.
    """

    def __init__(self, baseurl: URL, username: str, password: Optional[str] = None):
        super().__init__()
        self._do_login(baseurl, username, password)

    def _do_login(
            self,
            baseurl: URL,
            username: str,
            password: Optional[str] = None,
    ) -> None:
        """
        Login via api.
        """
        login_response = self.session.post(
            baseurl / "api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
                "refresh": True,
            },
        )
        access_token = login_response.json()['access_token']

        self.headers = {"Authorization": f"Bearer {access_token}"}
