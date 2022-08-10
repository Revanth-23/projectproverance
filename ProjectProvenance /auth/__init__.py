#
# Copyright (c) 2022 Resilinc, Inc., All Rights Reserved.
# Any use, modification, or distribution is prohibited
# without prior written consent from Resilinc, Inc.
#

# stdlib Imports.
import ast
import os
from enum import Enum
from typing import Any

# Third Party Imports.

# Local Imports.
import asyncpg

from configuration import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, REDIS_AUTH_URI
from managers.ResilincRedis.redis_client import RedisClient

# TODO: Dynamically Get resilinc tenant_id
RESILINC_TENANT_ID = 1


class AuthConstants(Enum):
    AUTHORIZATION = 'Authorization'
    SYSTEM_ACCESS_TOKEN = 'SYSTEM_ACCESS_TOKEN'
    EXCLUDE_FROM_AUTHENTICATION = '_exclude_from_authentication'
    LOGGED_IN_USER = 'logged_in_user'
    HEALTHCHECK_ENDPOINT = 'check'


class AuthUserConstants(Enum):
    USER_ANCHOR_ID = 'userId'
    TENANT_ID = 'tenantId'
    CURRENT_ORGANIZATION = 'currentOrganization'
    USERNAME = 'userName'
    FIRST_NAME = 'firstName'
    LAST_NAME = 'lastName'
    EMAIL = 'email'
    CURRENT_ROLE = 'currentRole'
    APPLICABLE_TENANTS = 'applicableTenants'
    APPLICABLE_ROLES = 'applicableRoles'
    PRIVATE_ID = 'privateId'
    USER_TYPE = 'userType'
    CUSTOMER_TYPE = 'customerType'
    KEEP_ME_LOGGED_IN = 'keepMeLoggedIn'
    ACCESS_TOKEN = 'accessToken'


class AuthUser(object):
    def __init__(self, user_anchor_id: str, tenant_id: str, current_organization: str, username: str, first_name: str,
                 last_name: str, email: str, current_role: str, applicable_tenants: Any, applicable_roles: str,
                 private_id: str, user_type: str, customer_type: str, keep_me_logged_in: str, access_token: str):
        self.user_anchor_id: int = int(user_anchor_id or 0)
        self.tenant_id: int = int(tenant_id or 0)
        self.current_organization: int = int(current_organization or 0)
        self.username: str = username or ''
        self.first_name: str = first_name or ''
        self.last_name: str = last_name or ''
        self.email: str = email or ''
        self.current_role: str = current_role or ''
        try:
            if applicable_tenants and isinstance(applicable_tenants, str):
                self.applicable_tenants = ast.literal_eval(applicable_tenants)
            elif applicable_tenants and isinstance(applicable_tenants, list):
                self.applicable_tenants = applicable_tenants
            else:
                self.applicable_tenants = []
        except ValueError:
            self.applicable_tenants = []
        except SyntaxError:
            self.applicable_tenants = []
            self.applicable_roles = applicable_roles or ''
        self.roles = self.applicable_roles.split(',') if self.applicable_roles else []
        self.private_id: int = int(private_id or 0)
        self.user_type: str = user_type or ''
        self.customer_type = customer_type
        if isinstance(keep_me_logged_in, str) and keep_me_logged_in:
            try:
                self.keep_me_logged_in = ast.literal_eval(keep_me_logged_in.lower().strip())
            except ValueError:
                self.keep_me_logged_in = False
            except SyntaxError:
                self.keep_me_logged_in = False
        elif isinstance(keep_me_logged_in, bool):
            self.keep_me_logged_in = keep_me_logged_in
        else:
            self.keep_me_logged_in = False
        self.access_token: str = access_token or ''

    def as_session_map(self) -> dict:
        """
        Convert an AuthUser Object to session map (as represented in session store).

        :return: dict
        """

        return {AuthUserConstants.USER_ANCHOR_ID.value: str(self.user_anchor_id),
                AuthUserConstants.TENANT_ID.value: str(self.tenant_id),
                AuthUserConstants.CURRENT_ORGANIZATION.value: str(self.current_organization),
                AuthUserConstants.USERNAME.value: str(self.username or ''),
                AuthUserConstants.FIRST_NAME.value: str(self.first_name or ''),
                AuthUserConstants.LAST_NAME.value: str(self.last_name or ''),
                AuthUserConstants.EMAIL.value: str(self.email or ''),
                AuthUserConstants.CURRENT_ROLE.value: str(self.current_role or 'NONE'),
                AuthUserConstants.APPLICABLE_TENANTS.value: str(self.applicable_tenants or []),
                AuthUserConstants.APPLICABLE_ROLES.value: str(self.applicable_roles or ''),
                AuthUserConstants.PRIVATE_ID.value: str(self.private_id or ''),
                AuthUserConstants.USER_TYPE.value: str(self.user_type or ''),
                AuthUserConstants.CUSTOMER_TYPE.value: str(self.customer_type or ''),
                AuthUserConstants.KEEP_ME_LOGGED_IN.value: str(self.keep_me_logged_in or False).lower()}

    @classmethod
    def session_map_to_auth_user(cls, session_map: dict, access_token: str):
        """
        Convert a session map to AuthUser Object.
        :param: session_map: Redis Session map.
        :type: session_map: dict
        :param: access_token: access_token to associate with Auth User object
        :return: AuthUser
        """

        return cls(session_map.get(AuthUserConstants.USER_ANCHOR_ID.value),
                   session_map.get(AuthUserConstants.TENANT_ID.value),
                   session_map.get(AuthUserConstants.CURRENT_ORGANIZATION.value),
                   session_map.get(AuthUserConstants.USERNAME.value),
                   session_map.get(AuthUserConstants.FIRST_NAME.value),
                   session_map.get(AuthUserConstants.LAST_NAME.value),
                   session_map.get(AuthUserConstants.EMAIL.value),
                   session_map.get(AuthUserConstants.CURRENT_ROLE.value),
                   session_map.get(AuthUserConstants.APPLICABLE_TENANTS.value),
                   session_map.get(AuthUserConstants.APPLICABLE_ROLES.value),
                   session_map.get(AuthUserConstants.PRIVATE_ID.value),
                   session_map.get(AuthUserConstants.USER_TYPE.value),
                   session_map.get(AuthUserConstants.CUSTOMER_TYPE.value),
                   session_map.get(AuthUserConstants.KEEP_ME_LOGGED_IN.value),
                   access_token=access_token)

    def is_a(self, role: str) -> bool:
        # TODO: Revisit/Deprecate/Change this implementation based on UM Revamp.
        if isinstance(role, str) and role:
            return role in self.roles
        return False

    def has_all_roles(self, roles: list or tuple) -> bool:
        if isinstance(roles, (list, tuple)):
            return all([role in self.roles for role in filter(lambda role: isinstance(role, str), filter(bool, roles))])
        return False

    def has_any_role(self, roles: list or tuple) -> bool:
        if isinstance(roles, (list, tuple)):
            return any([role in self.roles for role in filter(lambda role: isinstance(role, str), filter(bool, roles))])
        return False

    def is_resilinc_user(self) -> bool:
        # TODO: Get Resilinc tenant_id dynamically. Revisit/Deprecate/Change this implementation based on UM Revamp.
        return str(self.tenant_id) == str(RESILINC_TENANT_ID)

    def __str__(self) -> str:
        return '{} ({}) => user_anchor_id: {}, tenant_id: {}'.format('AuthUser', self.access_token, self.user_anchor_id,
                                                                     self.current_organization)

    def __repr__(self) -> str:
        return '{} ({}) => user_anchor_id: {}, tenant_id: {}'.format('AuthUser', self.access_token, self.user_anchor_id,
                                                                     self.current_organization)


class AuthUtils:
    redis_client = RedisClient.from_url(REDIS_AUTH_URI, decode_responses=True)
    SYSTEM_ACCESS_TOKEN: str = os.environ.get(AuthConstants.SYSTEM_ACCESS_TOKEN.value)

    @staticmethod
    def set_user_info_to_redis(access_token: str, user: AuthUser, ttl: int = -1) -> bool:
        """
        Set a user(AuthUser) as session map in Redis.

        :param: access_token: Access Token to set.
        :type: access_token: str
        :param: user: AuthUser Object.
        :type: user: AuthUser
        :param: ttl: Time to live (in seconds) for Access Token in session store..
        :type: ttl: int
        :return: bool
        """

        return AuthUtils.redis_client.hmset(name=access_token, mapping=user.as_session_map(), ttl=ttl)

    @staticmethod
    def get_user_info_from_redis(access_token: str) -> AuthUser or None:
        """
        Fetch user info from session store using access token, if token exists user info is returned as AuthUser object
        Else None.

        :param: access_token: Access Token for which user info is needed.
        :type: access_token: str
        :return: AuthUser
        """

        session_map = AuthUtils.redis_client.hgetall(access_token)
        if session_map:
            return AuthUser.session_map_to_auth_user(session_map, access_token)
        return None

    @staticmethod
    def get_user_info_as_dict(access_token: str) -> dict:
        """
        Get user info as dict object. If user doesn't exist in session store, empty dict is returned.

        :param: access_token: Access Token for which user info is needed.
        :type: access_token: str
        :return: dict
        """

        user = AuthUtils.get_user_info_from_redis(access_token)
        return vars(user) if user else dict()

    @staticmethod
    def get_user_info_as_session_mapping(access_token: str) -> dict:
        """
        Get user info as it is represented in session store.
        :param: access_token: Access Token for which user info is needed.
        :type: access_token: str
        :return: dict
        """

        user = AuthUtils.get_user_info_from_redis(access_token)
        return user.as_session_map() if user else dict()

    @staticmethod
    def get_system_token_info() -> AuthUser:
        """
        Get information of System Token in session store.

        :return: AuthUser
        """

        return AuthUtils.get_user_info_from_redis(AuthUtils.SYSTEM_ACCESS_TOKEN)

    @staticmethod
    async def set_system_token() -> bool:
        """
        Set System Token to session store. This system token can be used in throughout the application.
        It is important to set System token in environment variable before calling this function, else Runtime Error
        is thrown.

        :raises: RuntimeError
        :return: None
        """
        pg_client = await asyncpg.create_pool(user=POSTGRES_USER, password=POSTGRES_PASSWORD, database=POSTGRES_DB,
                                              host=POSTGRES_HOST, port=POSTGRES_PORT)
        async with pg_client.acquire() as conn:
            system_user_info = await conn.fetchrow("SELECT * FROM user_shareable where user_anchor_id = 1")
            first_name = system_user_info.get(
                'first_name') if system_user_info.get('first_name') is not None else 'System'
            last_name = system_user_info.get('last_name') if system_user_info.get('last_name') is not None else 'Code'
            email = system_user_info.get('email') or 'no-reply@resilinc.com'

        if AuthUtils.SYSTEM_ACCESS_TOKEN is None:
            raise RuntimeError("SYSTEM ACCESS TOKEN is not provided in Environment Variable.")

        system_user: AuthUser = AuthUser('1', '1', '1', 'system_user', first_name, last_name, email,
                                         '', '', '', '', '', '', 'false',
                                         AuthUtils.SYSTEM_ACCESS_TOKEN if AuthUtils.SYSTEM_ACCESS_TOKEN else '')
        return AuthUtils.set_user_info_to_redis(AuthUtils.SYSTEM_ACCESS_TOKEN, system_user)
