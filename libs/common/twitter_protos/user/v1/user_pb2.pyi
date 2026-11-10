from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class User(_message.Message):
    __slots__ = ("id", "username", "display_name", "bio", "created_at_unix", "followers_count", "following_count")
    ID_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    BIO_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    FOLLOWERS_COUNT_FIELD_NUMBER: _ClassVar[int]
    FOLLOWING_COUNT_FIELD_NUMBER: _ClassVar[int]
    id: str
    username: str
    display_name: str
    bio: str
    created_at_unix: int
    followers_count: int
    following_count: int
    def __init__(self, id: _Optional[str] = ..., username: _Optional[str] = ..., display_name: _Optional[str] = ..., bio: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., followers_count: _Optional[int] = ..., following_count: _Optional[int] = ...) -> None: ...

class GetUserRequest(_message.Message):
    __slots__ = ("user_id",)
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    def __init__(self, user_id: _Optional[str] = ...) -> None: ...

class BatchGetUsersRequest(_message.Message):
    __slots__ = ("user_ids",)
    USER_IDS_FIELD_NUMBER: _ClassVar[int]
    user_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, user_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class BatchGetUsersResponse(_message.Message):
    __slots__ = ("users",)
    class UsersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: User
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[User, _Mapping]] = ...) -> None: ...
    USERS_FIELD_NUMBER: _ClassVar[int]
    users: _containers.MessageMap[str, User]
    def __init__(self, users: _Optional[_Mapping[str, User]] = ...) -> None: ...

class ListFollowersRequest(_message.Message):
    __slots__ = ("user_id", "limit")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    limit: int
    def __init__(self, user_id: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class IsFollowingRequest(_message.Message):
    __slots__ = ("follower_id", "followee_id")
    FOLLOWER_ID_FIELD_NUMBER: _ClassVar[int]
    FOLLOWEE_ID_FIELD_NUMBER: _ClassVar[int]
    follower_id: str
    followee_id: str
    def __init__(self, follower_id: _Optional[str] = ..., followee_id: _Optional[str] = ...) -> None: ...

class IsFollowingResponse(_message.Message):
    __slots__ = ("following",)
    FOLLOWING_FIELD_NUMBER: _ClassVar[int]
    following: bool
    def __init__(self, following: _Optional[bool] = ...) -> None: ...
