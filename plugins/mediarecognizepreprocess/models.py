from dataclasses import dataclass, field
from typing import Dict, List, Optional, Pattern


@dataclass
class CleanupProfile:
    strip_patterns: List[str] = field(default_factory=list)
    release_tags: List[str] = field(default_factory=list)
    language_tags: List[str] = field(default_factory=list)
    subtitle_tags: List[str] = field(default_factory=list)
    audio_tags: List[str] = field(default_factory=list)
    misc_tags: List[str] = field(default_factory=list)
    trailing_patterns: List[str] = field(default_factory=list)
    bracket_patterns: List[str] = field(default_factory=list)
    strip_urls: bool = True
    compiled_strip_patterns: List[Pattern[str]] = field(default_factory=list, repr=False)
    compiled_bracket_patterns: List[Pattern[str]] = field(default_factory=list, repr=False)
    compiled_trailing_patterns: List[Pattern[str]] = field(default_factory=list, repr=False)
    compiled_release_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_language_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_subtitle_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_audio_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_misc_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_trailing_tag_pattern: Optional[Pattern[str]] = field(default=None, repr=False)


@dataclass
class RuleDef:
    name: str
    pattern: str
    media_type: str = "movie"
    use_path: bool = False
    filename_fallback: bool = False
    high_risk_mode: str = "apply"
    cleanup_profile: Optional[str] = None
    title_group: str = "title"
    year_group: Optional[str] = "year"
    season_group: Optional[str] = None
    episode_group: Optional[str] = None
    strip_patterns: List[str] = field(default_factory=list)
    compiled_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
    compiled_strip_patterns: List[Pattern[str]] = field(default_factory=list, repr=False)


@dataclass
class OverrideDef:
    keyword: str
    title: str
    year: Optional[str] = None
    media_type: str = "movie"
    tmdbid: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    directories: List[str] = field(default_factory=list)
    match_mode: str = "contains"
    match_on: str = "both"
    case_sensitive: bool = False
    high_risk_mode: str = "apply"
    compiled_pattern: Optional[Pattern[str]] = field(default=None, repr=False)


@dataclass
class MetaSource:
    source_path: Optional[str]
    raw_name: str
    current_name: Optional[str] = None


@dataclass
class ParseResult:
    title: str
    year: Optional[str] = None
    media_type: str = "movie"
    tmdbid: Optional[int] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    matched_by: str = ""
    matched_rule: Optional[str] = None
    high_risk: bool = False


@dataclass
class VarietyEpisodeTemplate:
    start_episode: int = 1
    issue_step: int = 1
    part_offsets: Dict[str, int] = field(default_factory=dict)


@dataclass
class VarietyEpisodeCase:
    date: Optional[str] = None
    issue: Optional[int] = None
    part_tag: Optional[str] = None
    title_keywords: List[str] = field(default_factory=list)
    season: Optional[int] = None
    episode: Optional[int] = None
    shift_following: bool = False


@dataclass
class VarietyEpisodeMapping:
    name: str
    tmdbid: Optional[int] = None
    title_keywords: List[str] = field(default_factory=list)
    directories: List[str] = field(default_factory=list)
    season: Optional[int] = None
    pattern: Optional[str] = None
    date_group: str = "date"
    issue_group: str = "issue"
    part_tag_group: str = "part_tag"
    template: Optional[VarietyEpisodeTemplate] = None
    special_fixed: List[VarietyEpisodeCase] = field(default_factory=list)
    special_inserts: List[VarietyEpisodeCase] = field(default_factory=list)
    compiled_pattern: Optional[Pattern[str]] = field(default=None, repr=False)
