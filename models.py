from dataclasses import dataclass

@dataclass
class FollowerRecord:
    """Represents a single Instagram follower's data."""
    username:     str
    full_name:    str
    user_id:      str
    biography:    str
    followers:    str
    following:    str
    media_count:  str
    is_private:   str
    is_verified:  str
    external_url: str
    profile_url:  str
