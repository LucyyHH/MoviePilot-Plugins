import copy
from contextvars import ContextVar
import importlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.plugins.mediarecognizepreprocess.parser import MediaRecognizePreprocessParser


_SAMPLE_DIRECTORY_RULES = (
    "/data/media/subscription/cloudpan189-share => variety_path_rules, tv_path_rules, movie_path_rules, movie_year_prefix"
)

_CONFIG_REVISION = 1

_SAMPLE_RULE_SETS = json.dumps(
    {
        "tv_path_rules": [
            {
                "name": "路径剧集-年份标题集数",
                "pattern": r".*/(?:[A-Za-z]\s+)?(?P<title>[^/（）()]+?)[（(](?P<year>20\d{2})[）)](?:\s*[^/]*)?.*?/(?:(?:4K|2160P|1080P)\s*更新/)?(?!(?:19|20)\d{2}[.\s_-])(?P<episode>\d{1,4})(?=[.\s_-]|$)",
                "type": "tv",
                "use_path": True,
                "cleanup_profile": "default_tv_cleanup",
                "title_group": "title",
                "year_group": "year",
                "episode_group": "episode",
            },
            {
                "name": "路径剧集-年份标题季集兼容",
                "pattern": r".*/(?:[A-Za-z]\s+)?(?P<title>[^/（）()]+?)[（(](?P<year>20\d{2})[）)](?:\s*[^/]*)?/(?:[^/]+/)*(?:S(?P<season>\d{1,2})[.\s_-]*)?(?!(?:19|20)\d{2}[.\s_-])(?P<episode>\d{1,4})(?=[.\s_-]|$)",
                "type": "tv",
                "use_path": True,
                "cleanup_profile": "default_tv_cleanup",
                "title_group": "title",
                "year_group": "year",
                "season_group": "season",
                "episode_group": "episode",
            },
            {
                "name": "路径剧集-父目录标题纯集号",
                "pattern": r".*/(?P<title>[^/（）()]+?)(?:\s+\d{1,4}(?:之后|以后|起|后续|续更|更新中))?/(?:(?:4K|2160P|1080P|720P)/)?(?P<episode>\d{1,4})(?:\s*(?:4K|2160P|1080P|720P))?$",
                "type": "tv",
                "use_path": True,
                "cleanup_profile": "default_tv_cleanup",
                "title_group": "title",
                "episode_group": "episode",
                "strip_patterns": [
                    r"\s+\d{1,4}(?:之后|以后|起|后续|续更|更新中)$"
                ],
            }
        ],
        "variety_path_rules": [
            {
                "name": "综艺-标题季-日期分段",
                "pattern": r".*/(?P<title>.+?)(?P<season>\d{1,2})/(?:(?P<date>\d{8})[.\s_-]*)?(?:第(?P<episode>\d+)\s*期\s*)?(?:先导片上|先导片下|特别加更|还有加更|加更|上|下)(?:[：:.\s_-].*)?$",
                "type": "tv",
                "use_path": True,
                "high_risk_mode": "warn",
                "cleanup_profile": "default_tv_cleanup",
                "title_group": "title",
                "season_group": "season",
                "episode_group": "episode",
                "strip_patterns": [
                    r"\s*第\s*$"
                ]
            }
        ],
        "movie_path_rules": [
            {
                "name": "路径电影-目录标题年份优先",
                "pattern": r"^.*/(?P<title>[^/（）()]+?)\s*[（(](?P<year>(19|20)\d{2})[）)](?:\s*[^/]*)?/(?:[^/]+)$",
                "type": "movie",
                "use_path": True,
                "high_risk_mode": "warn",
                "cleanup_profile": "default_movie_cleanup",
                "title_group": "title",
                "year_group": "year",
            }
        ],
        "movie_year_prefix": [
            {
                "name": "前缀年份电影-严格兜底",
                "pattern": r"^(?P<year>(19|20)\d{2})[.\s_-]+(?P<title>(?!\d{3,4}(?:p|k)\b)(?!uhd\b)(?!bluray\b)(?!web[.\s_-]?dl\b)(?!remux\b)(?!hevc\b)(?!hdr10?\b)(?!dv\b)(?!dovi\b)(?!truehd\b)(?!atmos\b)(?!\d+(?:\.\d+)?$)(?=.*[\u4e00-\u9fffA-Za-z]).+)$",
                "type": "movie",
                "filename_fallback": True,
                "high_risk_mode": "warn",
                "cleanup_profile": "default_movie_cleanup",
                "title_group": "title",
                "year_group": "year",
            }
        ]
    },
    ensure_ascii=False,
    indent=2,
)

_SAMPLE_CLEANUP_PROFILES = json.dumps(
    {
        "default_movie_cleanup": {
            "release_tags": [
                "BD",
                "BDRip",
                "BluRay",
                "WEB-DL",
                "REMUX",
                "UHD",
                "4K",
                "4320P",
                "2160P",
                "1080P",
                "720P",
                "X264",
                "X265",
                "H264",
                "H265",
                "HEVC",
                "AVC",
            ],
            "language_tags": [
                "国语",
                "粤语",
                "英语",
                "英字",
                "日语",
                "韩语",
                "西班牙语",
                "丹麦语",
                "德语",
                "法语",
                "俄语",
                "泰语",
                "印度语",
                "印地语",
                "意大利语",
                "葡萄牙语",
                "中文字幕",
            ],
            "subtitle_tags": [
                "中英双字",
                "中英双语字幕",
                "内嵌中字",
                "简繁英双字",
                "简繁英双语字幕",
                "简繁中字",
                "简中",
                "繁中",
                "中字",
                "双字",
                "字幕",
                "特效字幕",
                "官方中字",
            ],
            "audio_tags": [
                "AAC",
                "AC3",
                "EAC3",
                "FLAC",
                "TRUEHD",
                "ATMOS",
                "DTS",
                "DTS-HD",
                "DD",
                "DDP",
                "5.1",
                "DD5.1",
                "DDP5.1",
                "7.1",
                "DD7.1",
                "DDP7.1",
            ],
            "misc_tags": [
                "PROPER",
                "REPACK",
                "EXTENDED",
                "COMPLETE",
                "UNRATED",
                "MULTI",
                "DoVi",
                "DUBBED",
                "HDR",
                "HDR10",
                "DV",
            ],
            "trailing_patterns": [
                "(?:[\\\\s._-]*[\\\\u4e00-\\\\u9fff]{1,6}语)+$"
            ],
            "bracket_patterns": [
                "\\\\[[^\\\\]]+\\\\]",
                "【[^】]+】",
                "\\\\([^\\\\)]*www[^\\\\)]*\\\\)"
            ],
            "strip_urls": True
        },
        "default_tv_cleanup": {
            "release_tags": [
                "4K",
                "2160P",
                "1080P",
                "720P",
                "WEB-DL",
                "BluRay",
                "HDTV",
                "X264",
                "X265",
                "H264",
                "H265",
                "HEVC"
            ],
            "language_tags": [
                "国语",
                "粤语",
                "英语",
                "日语",
                "韩语"
            ],
            "subtitle_tags": [
                "中字",
                "中英双字",
                "内嵌中字"
            ],
            "audio_tags": [],
            "misc_tags": [
                "无台标",
                "更新",
                "完结",
                "全集"
            ],
            "trailing_patterns": [
                "(?:[\\\\s._-]*[\\\\u4e00-\\\\u9fff]{1,6}语)+$",
                "(?:[\\\\s._-]*共\\\\d+集)$"
            ],
            "bracket_patterns": [
                "\\\\[[^\\\\]]+\\\\]",
                "【[^】]+】"
            ],
            "strip_urls": True
        }
    },
    ensure_ascii=False,
    indent=2,
)

_SAMPLE_OVERRIDES = json.dumps(
    [
        {
            "keyword": "电影示例别名",
            "title": "电影标准名",
            "year": "2024",
            "type": "movie",
            "match_mode": "contains",
            "match_on": "both",
            "case_sensitive": False,
            "directories": [
                "/data/media/subscription"
            ],
        }
    ],
    ensure_ascii=False,
    indent=2,
)

_SAMPLE_HISTORY_CONTEXT_METHODS = json.dumps(
    [
        "get_by_id",
        "get_by_src",
        "get_by_dest",
        "get_by_type_tmdbid",
    ],
    ensure_ascii=False,
    indent=2,
)

_SAMPLE_HISTORY_CONTEXT_METHOD_LINES = "\n".join(
    [
        "get_by_id",
        "get_by_src",
        "get_by_dest",
        "get_by_type_tmdbid",
    ]
)

_METAINFO_TARGETS = (
    ("app.core.metainfo", "_hook_recognize_media"),
    ("app.plugins.dirmonitor", "_hook_dirmonitor"),
    ("app.chain.transfer", "_hook_recognize_media"),
)

_EPISODE_PREFIX_PATTERN = re.compile(r"^(?P<episode>\d{1,3})(?=[.\s_-]|$)")
_HISTORY_KEY_SEPARATOR_PATTERN = re.compile(r"[\s._-]+")
_HISTORY_PATH_PROBE_PATTERN = re.compile(r"/\d{1,3}\.[^/]+$")

_SAMPLE_VARIETY_EPISODE_MAPPINGS = json.dumps(
    [
        {
            "name": "现在就出发3",
            "title_keywords": [
                "现在就出发"
            ],
            "directories": [
                "/data/media/subscription/cloudpan189-share"
            ],
            "season": 3,
            "pattern": r"^(?:(?P<date>\d{8})[.\s_-]*)?(?:第(?P<issue>\d+)\s*期\s*)?(?P<part_tag>先导片上|先导片下|特别加更|还有加更|加更|上|下)(?:[：:.\s_-].*)?$",
            "template": {
                "start_episode": 3,
                "issue_step": 4,
                "part_offsets": {
                    "上": 0,
                    "下": 1,
                    "加更": 2,
                    "还有加更": 3
                }
            },
            "special_fixed": [
                {
                    "part_tag": "先导片上",
                    "title_keywords": [
                        "显眼包们开启沈腾模仿大赛"
                    ],
                    "episode": 1
                },
                {
                    "part_tag": "先导片下",
                    "title_keywords": [
                        "贾冰厨房营业",
                        "沈腾吃到晕碳"
                    ],
                    "episode": 2
                }
            ],
            "special_inserts": [
                {
                    "date": "20251127",
                    "part_tag": "特别加更",
                    "title_keywords": [
                        "一场酣畅淋漓的粤语全障碍对话"
                    ],
                    "episode": 23,
                    "shift_following": True
                },
                {
                    "date": "20251211",
                    "part_tag": "特别加更",
                    "title_keywords": [
                        "蒙古舞是沈腾的自由舒适区"
                    ],
                    "episode": 32,
                    "shift_following": True
                },
                {
                    "date": "20260104",
                    "part_tag": "特别加更",
                    "title_keywords": [
                        "沈腾跳扫腿舞“性感”wave"
                    ],
                    "episode": 45,
                    "shift_following": True
                },
                {
                    "date": "20260105",
                    "part_tag": "特别加更",
                    "title_keywords": [
                        "沈腾黄景瑜变御姐音"
                    ],
                    "episode": 46,
                    "shift_following": True
                },
                {
                    "date": "20260106",
                    "part_tag": "特别加更",
                    "title_keywords": [
                        "吻戏大赏！腾腾公主驾到"
                    ],
                    "episode": 47,
                    "shift_following": True
                }
            ]
        }
    ],
    ensure_ascii=False,
    indent=2,
)


class MediaRecognizePreprocess(_PluginBase):
    plugin_name = "媒体识别预处理"
    plugin_desc = "在目录监控、手动整理等识别前，按路径、文件名和清洗规则修正媒体识别输入，不改源文件名。"
    plugin_icon = "scraper.png"
    plugin_version = "1.0.0"
    plugin_author = "LucyyHH"
    author_url = ""
    plugin_config_prefix = "mediarecognizepreprocess_"
    plugin_order = 23
    auth_level = 1

    _enabled: bool = False
    _hook_dirmonitor: bool = True
    _hook_recognize_media: bool = True
    _only_strm: bool = True
    _config_revision: int = _CONFIG_REVISION
    _directory_rules: str = _SAMPLE_DIRECTORY_RULES
    _rule_sets: str = _SAMPLE_RULE_SETS
    _cleanup_profiles: str = _SAMPLE_CLEANUP_PROFILES
    _overrides: str = "[]"
    _variety_episode_mappings: str = _SAMPLE_VARIETY_EPISODE_MAPPINGS
    _history_context_methods: str = _SAMPLE_HISTORY_CONTEXT_METHOD_LINES

    _parser: Optional[MediaRecognizePreprocessParser] = None
    _original_metainfo_targets: Dict[str, Tuple[Any, Any]] = {}
    _original_downloadhis_get_file_by_fullpath = None
    _original_chain_recognize_media = None
    _original_chain_async_recognize_media = None
    _original_transferhis_methods: Dict[str, Any] = {}
    _history_src_context: ContextVar = ContextVar("mediarecognizepreprocess_history_src", default=())
    _dirmonitor_bypass_context: ContextVar = ContextVar(
        "mediarecognizepreprocess_dirmonitor_bypass",
        default=(),
    )
    _history_context_method_names: List[str] = []

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._clear_history_src_context()
        self._clear_dirmonitor_bypass_context()

        self._enabled = False
        self._hook_dirmonitor = True
        self._hook_recognize_media = True
        self._only_strm = True
        self._config_revision = _CONFIG_REVISION
        self._directory_rules = _SAMPLE_DIRECTORY_RULES
        self._rule_sets = _SAMPLE_RULE_SETS
        self._cleanup_profiles = _SAMPLE_CLEANUP_PROFILES
        self._overrides = "[]"
        self._variety_episode_mappings = _SAMPLE_VARIETY_EPISODE_MAPPINGS
        self._history_context_methods = _SAMPLE_HISTORY_CONTEXT_METHOD_LINES
        self._history_context_method_names = []

        if config:
            config_revision = self._get_int_config(config, "config_revision")
            should_reset_to_defaults = config_revision != _CONFIG_REVISION
            if should_reset_to_defaults:
                logger.info(
                    f"{self.plugin_name} 检测到旧版配置修订："
                    f"stored={config_revision}, current={_CONFIG_REVISION}，"
                    "已回退到当前源码默认配置"
                )
            else:
                self._enabled = self._get_bool_config(config, "enabled", False)
                self._hook_dirmonitor = self._get_bool_config(
                    config,
                    "hook_dirmonitor",
                    True,
                    legacy_key="recognize_media",
                )
                self._hook_recognize_media = self._get_bool_config(config, "hook_recognize_media", True)
                self._only_strm = self._get_bool_config(config, "only_strm", True)
                self._directory_rules = self._get_config_value(config, "directory_rules", _SAMPLE_DIRECTORY_RULES)
                self._rule_sets = self._get_config_value(config, "rule_sets", _SAMPLE_RULE_SETS)
                self._cleanup_profiles = self._get_config_value(config, "cleanup_profiles", _SAMPLE_CLEANUP_PROFILES)
                self._overrides = self._get_config_value(config, "overrides", "[]")
                self._variety_episode_mappings = self._get_config_value(
                    config,
                    "variety_episode_mappings",
                    _SAMPLE_VARIETY_EPISODE_MAPPINGS,
                )
                self._history_context_methods = self._get_config_value(
                    config,
                    "history_context_methods",
                    _SAMPLE_HISTORY_CONTEXT_METHOD_LINES,
                )

        self._history_context_method_names = self._parse_history_context_methods(self._history_context_methods)

        self._parser = MediaRecognizePreprocessParser(
            directory_rules_text=self._directory_rules,
            rule_sets_text=self._rule_sets,
            cleanup_profiles_text=self._cleanup_profiles,
            overrides_text=self._overrides,
            variety_episode_mappings_text=self._variety_episode_mappings,
            only_strm=self._only_strm,
        )
        self._update_config()

        if self._enabled and (self._hook_dirmonitor or self._hook_recognize_media):
            self._patch_metainfo_targets()
        if self._enabled and self._hook_dirmonitor:
            self._patch_download_history_lookup()
        if self._enabled and self._hook_recognize_media:
            self._patch_transfer_history_context()
            self._patch_recognize_media()

    def _update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "hook_dirmonitor": self._hook_dirmonitor,
                "hook_recognize_media": self._hook_recognize_media,
                "only_strm": self._only_strm,
                "config_revision": self._config_revision,
                "directory_rules": self._directory_rules,
                "rule_sets": self._rule_sets,
                "cleanup_profiles": self._cleanup_profiles,
                "overrides": self._overrides,
                "variety_episode_mappings": self._variety_episode_mappings,
                "history_context_methods": self._history_context_methods,
            }
        )

    @staticmethod
    def _get_config_value(config: dict, key: str, default, legacy_key: str = None):
        if key in config:
            value = config.get(key)
            return default if value is None else value
        if legacy_key and legacy_key in config:
            value = config.get(legacy_key)
            return default if value is None else value
        return default

    @staticmethod
    def _get_bool_config(config: dict, key: str, default: bool, legacy_key: str = None) -> bool:
        value = MediaRecognizePreprocess._get_config_value(config, key, default, legacy_key=legacy_key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "1", "yes", "on"):
                return True
            if lowered in ("false", "0", "no", "off"):
                return False
        return bool(value)

    @staticmethod
    def _get_int_config(config: dict, key: str, legacy_key: str = None) -> Optional[int]:
        value = MediaRecognizePreprocess._get_config_value(config, key, None, legacy_key=legacy_key)
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _parse_history_context_methods(self, text: str) -> List[str]:
        if not text:
            return []
        text = str(text).strip()
        if not text:
            return []

        if text.startswith("["):
            try:
                raw = json.loads(text)
            except json.JSONDecodeError as err:
                logger.error(f"{self.plugin_name} 历史上下文方法配置JSON解析失败：{err}")
                return []
            if not isinstance(raw, list):
                logger.error(f"{self.plugin_name} 历史上下文方法配置必须是JSON数组")
                return []
            return self._dedupe_method_names(raw)

        lines: List[str] = []
        for raw_line in text.splitlines():
            line = str(raw_line).strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
        return self._dedupe_method_names(lines)

    @staticmethod
    def _dedupe_method_names(items) -> List[str]:
        result: List[str] = []
        for item in items:
            name = str(item).strip()
            if name and name not in result:
                result.append(name)
        return result

    def _patch_metainfo_targets(self):
        patched_modules: List[str] = []
        for module_name, hook_attr in self._iter_metainfo_target_modules():
            if self._patch_metainfo_target(module_name=module_name, hook_attr=hook_attr):
                patched_modules.append(module_name)

        if patched_modules:
            logger.info(
                f"{self.plugin_name} 已接管MetaInfoPath前置解析：modules={patched_modules}"
            )

    def _iter_metainfo_target_modules(self) -> List[Tuple[str, str]]:
        return list(_METAINFO_TARGETS)

    def _patch_metainfo_target(self, module_name: str, hook_attr: str) -> bool:
        try:
            module = importlib.import_module(module_name)
        except Exception as err:
            if module_name in ("app.plugins.dirmonitor", "app.chain.transfer"):
                logger.warning(f"{self.plugin_name} 导入MetaInfoPath目标失败：module={module_name}, err={err}")
            return False

        original_metainfo = getattr(module, "MetaInfoPath", None)
        if not callable(original_metainfo):
            return False
        if getattr(original_metainfo, "_patched_by", object()) == id(self):
            return False

        if module_name not in self._original_metainfo_targets:
            self._original_metainfo_targets[module_name] = (module, original_metainfo)
        else:
            _, original_metainfo = self._original_metainfo_targets[module_name]

        plugin_instance = self

        def patched_metainfo(*args, **kwargs):
            meta = original_metainfo(*args, **kwargs)
            if not plugin_instance._should_apply_metainfo_rewrite(hook_attr):
                return meta

            if hook_attr == "_hook_dirmonitor":
                plugin_instance._clear_dirmonitor_bypass_context()

            path = plugin_instance._extract_metainfo_path(args=args, kwargs=kwargs, meta=meta)
            if not path:
                return meta
            return plugin_instance._rewrite_metainfo_for_path(
                meta=meta,
                path=path,
                hook_attr=hook_attr,
            )

        setattr(patched_metainfo, "_patched_by", id(self))
        setattr(patched_metainfo, "_patched_module", module_name)
        module.MetaInfoPath = patched_metainfo
        return True

    def _patch_download_history_lookup(self):
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
        except Exception as err:
            logger.error(f"{self.plugin_name} 导入下载历史依赖失败：{err}")
            return

        if self._original_downloadhis_get_file_by_fullpath is None:
            self._original_downloadhis_get_file_by_fullpath = DownloadHistoryOper.get_file_by_fullpath

        plugin_instance = self
        original_get_file_by_fullpath = self._original_downloadhis_get_file_by_fullpath

        def patched_get_file_by_fullpath(downloadhis_self, fullpath: str):
            if (
                plugin_instance._enabled
                and plugin_instance._hook_dirmonitor
                and fullpath
                and plugin_instance._consume_dirmonitor_bypass_context(fullpath)
            ):
                file_path = Path(fullpath)
                logger.info(
                    f"{plugin_instance.plugin_name} 命中目录前置规则，跳过下载历史TMDB识别：{file_path}"
                )
                return None
            return original_get_file_by_fullpath(downloadhis_self, fullpath)

        setattr(patched_get_file_by_fullpath, "_patched_by", id(self))
        DownloadHistoryOper.get_file_by_fullpath = patched_get_file_by_fullpath
        logger.info(f"{self.plugin_name} 已接管下载历史旁路判断")

    def _should_apply_metainfo_rewrite(self, hook_attr: str) -> bool:
        return bool(self._enabled and self._parser and getattr(self, hook_attr, False))

    def _rewrite_metainfo_for_path(self, meta, path, hook_attr: Optional[str] = None):
        file_path = Path(path)
        result, source = self._parser.match_path(file_path)
        if not result or not source:
            return meta

        if hook_attr == "_hook_dirmonitor":
            self._set_dirmonitor_bypass_context(source.source_path or str(file_path))

        rewritten = self._parser.apply(
            meta,
            result,
            fallback_type=getattr(meta, "type", None),
        )
        logger.info(
            f"{self.plugin_name} 命中MetaInfoPath前置规则："
            f"path={file_path}, title={result.title}, year={result.year}, mode={result.matched_by}"
        )
        return rewritten

    def _extract_metainfo_path(self, args, kwargs, meta) -> Optional[str]:
        candidates = []
        if args:
            candidates.append(args[0])
        for key in ("path", "file_path", "filepath", "fullpath", "src_path", "source_path"):
            if key in kwargs:
                candidates.append(kwargs.get(key))

        for value in candidates:
            normalized = self._normalize_metainfo_path(value)
            if normalized:
                return normalized

        if self._parser:
            source = self._parser.extract_meta_source(meta)
            if source and source.source_path:
                return source.source_path
        return None

    def _normalize_metainfo_path(self, value) -> Optional[str]:
        if not self._parser:
            return None
        try:
            return self._parser._normalize_path(value)
        except Exception:
            return None

    def _set_dirmonitor_bypass_context(self, source_path: str, limit: int = 8) -> None:
        normalized = self._normalize_metainfo_path(source_path)
        if normalized:
            current = [item for item in self._dirmonitor_bypass_context.get() if item != normalized]
            current.insert(0, normalized)
            self._dirmonitor_bypass_context.set(tuple(current[:limit]))

    def _consume_dirmonitor_bypass_context(self, source_path: str) -> bool:
        normalized = self._normalize_metainfo_path(source_path)
        current = list(self._dirmonitor_bypass_context.get() or ())
        if not normalized or not current or normalized not in current:
            return False
        current = [item for item in current if item != normalized]
        self._dirmonitor_bypass_context.set(tuple(current))
        return True

    def _clear_dirmonitor_bypass_context(self) -> None:
        self._dirmonitor_bypass_context.set(())

    def _patch_recognize_media(self):
        try:
            from app.chain import ChainBase
        except Exception as err:
            logger.error(f"{self.plugin_name} 导入识别链依赖失败：{err}")
            return

        if self._original_chain_recognize_media is None:
            self._original_chain_recognize_media = getattr(ChainBase, "recognize_media", None)
        if self._original_chain_async_recognize_media is None:
            self._original_chain_async_recognize_media = getattr(ChainBase, "async_recognize_media", None)

        plugin_instance = self
        original_recognize_media = self._original_chain_recognize_media
        original_async_recognize_media = self._original_chain_async_recognize_media

        def patched_recognize_media(
            chain_self,
            meta=None,
            mtype=None,
            tmdbid=None,
            doubanid=None,
            bangumiid=None,
            episode_group=None,
            cache=True,
        ):
            if not original_recognize_media:
                return None
            rewritten_meta = plugin_instance._rewrite_meta_for_recognize(
                meta=meta,
                scene="recognize_media",
                mtype=mtype,
                tmdbid=tmdbid,
                doubanid=doubanid,
                bangumiid=bangumiid,
            )
            return original_recognize_media(
                chain_self,
                rewritten_meta,
                mtype,
                tmdbid,
                doubanid,
                bangumiid,
                episode_group,
                cache,
            )

        async def patched_async_recognize_media(
            chain_self,
            meta=None,
            mtype=None,
            tmdbid=None,
            doubanid=None,
            bangumiid=None,
            episode_group=None,
            cache=True,
        ):
            if not original_async_recognize_media:
                return None
            rewritten_meta = plugin_instance._rewrite_meta_for_recognize(
                meta=meta,
                scene="async_recognize_media",
                mtype=mtype,
                tmdbid=tmdbid,
                doubanid=doubanid,
                bangumiid=bangumiid,
            )
            return await original_async_recognize_media(
                chain_self,
                rewritten_meta,
                mtype,
                tmdbid,
                doubanid,
                bangumiid,
                episode_group,
                cache,
            )

        setattr(patched_recognize_media, "_patched_by", id(self))
        setattr(patched_async_recognize_media, "_patched_by", id(self))
        ChainBase.recognize_media = patched_recognize_media
        ChainBase.async_recognize_media = patched_async_recognize_media
        logger.info(f"{self.plugin_name} 已接管识别链前置解析")

    def _patch_transfer_history_context(self):
        try:
            from app.db.transferhistory_oper import TransferHistoryOper
        except Exception as err:
            logger.error(f"{self.plugin_name} 导入转移历史依赖失败：{err}")
            return

        plugin_instance = self
        patched_count = 0

        if not self._history_context_method_names:
            logger.info(f"{self.plugin_name} 未配置历史上下文方法，跳过转移历史上下文接管")
            return

        for method_name in self._history_context_method_names:
            current_method = getattr(TransferHistoryOper, method_name, None)
            if not callable(current_method):
                logger.warning(
                    f"{self.plugin_name} 历史上下文方法不存在或不可调用，已忽略：{method_name}"
                )
                continue

            if method_name not in self._original_transferhis_methods:
                self._original_transferhis_methods[method_name] = current_method
            original_method = self._original_transferhis_methods[method_name]

            def make_patched(name, original):
                def patched(history_self, *args, **kwargs):
                    result = original(history_self, *args, **kwargs)
                    if plugin_instance._enabled and plugin_instance._hook_recognize_media:
                        plugin_instance._capture_transfer_history_context(result, scene=name)
                    return result

                setattr(patched, "_patched_by", id(plugin_instance))
                setattr(patched, "_patched_name", name)
                return patched

            setattr(TransferHistoryOper, method_name, make_patched(method_name, original_method))
            patched_count += 1

        logger.info(f"{self.plugin_name} 已接管转移历史读取上下文：patched={patched_count}")

    def _rewrite_meta_for_recognize(
        self,
        meta,
        scene: str,
        mtype=None,
        tmdbid=None,
        doubanid=None,
        bangumiid=None,
    ):
        if not meta or not self._enabled or not self._hook_recognize_media or not self._parser:
            return meta

        result, source = self._parser.match(meta, log_misses=False)
        if result and source:
            rewritten = self._parser.apply(
                meta,
                result,
                fallback_type=getattr(meta, "type", None) or mtype,
            )
            logger.info(
                f"{self.plugin_name} 命中识别链前置规则：scene={scene}, "
                f"source_path={source.source_path}, raw_name={source.raw_name}, "
                f"title={result.title}, year={result.year}, mode={result.matched_by}"
            )
            self._clear_history_src_context()
            return rewritten

        if source:
            episode_fallback = self._apply_manual_episode_fallback(
                meta=meta,
                source=source,
                scene=scene,
                tmdbid=tmdbid,
                doubanid=doubanid,
                bangumiid=bangumiid,
            )
            if episode_fallback is not None:
                self._clear_history_src_context()
                return episode_fallback

        history_rewrite = self._rewrite_from_history_context(
            meta=meta,
            source=source,
            scene=scene,
            mtype=mtype,
        )
        if history_rewrite is not None:
            self._clear_history_src_context()
            return history_rewrite

        if source and self._should_probe_source(source):
            logger.info(
                f"{self.plugin_name} 识别链探测：scene={scene}, "
                f"source_path={source.source_path}, raw_name={source.raw_name}, "
                f"current_name={source.current_name}, meta={self._parser._debug_meta_snapshot(meta)}"
            )
        elif not source and self._should_probe_meta(meta):
            logger.info(
                f"{self.plugin_name} 识别链探测：scene={scene}, "
                f"未提取到路径或原始名，meta={self._parser._debug_meta_snapshot(meta)}"
            )
        self._clear_history_src_context()
        return meta

    def _rewrite_from_history_context(
        self,
        meta,
        source,
        scene: str,
        mtype=None,
    ):
        history_src = self._find_matching_history_src(source)
        if not history_src:
            return None

        history_result, history_source = self._parser.match_path(Path(history_src), log_misses=False)
        if history_result and history_source:
            rewritten = self._parser.apply(
                meta,
                history_result,
                fallback_type=getattr(meta, "type", None) or mtype,
            )
            self._attach_source_path(rewritten, history_src)
            logger.info(
                f"{self.plugin_name} 命中历史源路径前置规则：scene={scene}, "
                f"history_src={history_src}, title={history_result.title}, "
                f"year={history_result.year}, mode={history_result.matched_by}"
            )
            return rewritten

        if self._should_probe_history_path(history_src):
            logger.info(
                f"{self.plugin_name} 历史源路径探测：scene={scene}, "
                f"history_src={history_src}, meta={self._parser._debug_meta_snapshot(meta)}"
            )
        return None

    def _apply_manual_episode_fallback(
        self,
        meta,
        source,
        scene: str,
        tmdbid=None,
        doubanid=None,
        bangumiid=None,
    ):
        if source.source_path or not (tmdbid or doubanid or bangumiid):
            return None

        raw_name = str(getattr(source, "raw_name", "") or "").strip()
        if not raw_name:
            return None

        match = _EPISODE_PREFIX_PATTERN.match(raw_name)
        if not match:
            return None

        try:
            rewritten = copy.deepcopy(meta)
        except Exception:
            rewritten = copy.copy(meta)

        try:
            setattr(rewritten, "begin_episode", int(match.group("episode")))
        except Exception:
            return None

        logger.info(
            f"{self.plugin_name} 命中无路径集数兜底：scene={scene}, "
            f"raw_name={raw_name}, episode={getattr(rewritten, 'begin_episode', None)}"
        )
        return rewritten

    def _capture_transfer_history_context(self, result, scene: str):
        src_paths = self._extract_transfer_history_srcs(result)
        if not src_paths:
            return
        for src_path in src_paths:
            self._push_history_src_context(src_path)
            if self._should_probe_history_path(src_path):
                logger.info(
                    f"{self.plugin_name} 捕获转移历史源路径：scene={scene}, history_src={src_path}"
                )

    def _extract_transfer_history_srcs(self, result) -> List[str]:
        if result is None:
            return []

        items: List[str] = []
        if isinstance(result, (list, tuple, set)):
            for item in result:
                items.extend(self._extract_transfer_history_srcs(item))
            return self._dedupe_history_srcs(items)

        if isinstance(result, dict):
            for key in ("src", "src_path", "source_path"):
                value = result.get(key)
                if value:
                    items.append(str(value))
            return self._dedupe_history_srcs(items)

        for attr in ("src", "src_path", "source_path"):
            value = getattr(result, attr, None)
            if value:
                items.append(str(value))
        return self._dedupe_history_srcs(items)

    @staticmethod
    def _dedupe_history_srcs(items: List[str]) -> List[str]:
        result: List[str] = []
        seen = set()
        for item in items:
            normalized = str(item).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _push_history_src_context(self, src_path: str, limit: int = 20) -> None:
        normalized = str(src_path).strip()
        if not normalized:
            return
        current = [item for item in self._history_src_context.get() if item != normalized]
        current.insert(0, normalized)
        self._history_src_context.set(tuple(current[:limit]))

    def _clear_history_src_context(self) -> None:
        self._history_src_context.set(())

    def _find_matching_history_src(self, source) -> Optional[str]:
        candidates = list(self._history_src_context.get() or ())
        if not candidates:
            return None
        if not source or not getattr(source, "raw_name", None):
            return candidates[0] if len(candidates) == 1 else None

        current_key = self._normalize_history_key(source.raw_name)
        for candidate in candidates:
            if self._normalize_history_key(Path(candidate).stem) == current_key:
                return candidate
        return None

    @staticmethod
    def _normalize_history_key(value: str) -> str:
        normalized = str(value or "").lower().replace("：", ":")
        return _HISTORY_KEY_SEPARATOR_PATTERN.sub("", normalized)

    @staticmethod
    def _attach_source_path(meta, source_path: str):
        for attr in ("src_path", "source_path", "path", "file_path", "fullpath"):
            try:
                setattr(meta, attr, str(source_path))
            except Exception:
                continue

    @staticmethod
    def _should_probe_history_path(source_path: str) -> bool:
        lowered = (source_path or "").lower()
        if lowered.endswith(".strm"):
            return True
        return bool(_HISTORY_PATH_PROBE_PATTERN.search(lowered))

    @staticmethod
    def _should_probe_source(source) -> bool:
        source_path = (getattr(source, "source_path", None) or "").lower()
        raw_name = getattr(source, "raw_name", None) or ""
        current_name = getattr(source, "current_name", None) or ""
        return (
            source_path.endswith(".strm")
            or bool(MediaRecognizePreprocessParser._YEAR_PREFIX_PATTERN.match(raw_name))
            or bool(MediaRecognizePreprocessParser._TIME_NAME_PATTERN.match(current_name))
        )

    @staticmethod
    def _should_probe_meta(meta) -> bool:
        name = str(getattr(meta, "name", None) or getattr(meta, "title", None) or "").strip()
        if not name:
            return False
        return (
            name.lower().endswith(".strm")
            or bool(MediaRecognizePreprocessParser._YEAR_PREFIX_PATTERN.match(name))
            or bool(MediaRecognizePreprocessParser._TIME_NAME_PATTERN.match(name))
        )

    def get_state(self) -> bool:
        return self._enabled and (self._hook_dirmonitor or self._hook_recognize_media)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/reset_defaults",
                "endpoint": self.reset_defaults,
                "methods": ["POST"],
                "summary": "重置为源码默认配置",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def reset_defaults(self, apikey: str) -> schemas.Response:
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        self.init_plugin(None)
        return schemas.Response(success=True, message="已重置为源码默认配置，请刷新插件配置页")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VCard",
                        "props": {"class": "mt-0"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "d-flex align-center"},
                                "content": [
                                    {
                                        "component": "span",
                                        "text": "运行与钩子",
                                    },
                                    {"component": "VSpacer"},
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "color": "warning",
                                            "variant": "tonal",
                                            "size": "small",
                                        },
                                        "text": "重装默认配置",
                                        "events": {
                                            "click": {
                                                "api": f"plugin/{self.__class__.__name__}/reset_defaults?apikey={settings.API_TOKEN}",
                                                "method": "post",
                                            }
                                        },
                                    },
                                ],
                            },
                            {"component": "VDivider"},
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 3},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "enabled",
                                                            "label": "启用插件",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 3},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "hook_dirmonitor",
                                                            "label": "目录监控前置解析",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 3},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "hook_recognize_media",
                                                            "label": "手动整理识别前置解析",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 3},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "only_strm",
                                                            "label": "仅处理STRM",
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "插件会在目录监控、手动整理和历史重整识别前尝试修正媒体识别输入，不会改动源文件名。"
                                                "当前版本收敛了补丁范围，只接管明确白名单入口。"
                                            ),
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"class": "mt-3"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "d-flex align-center"},
                                "content": [
                                    {
                                        "component": "span",
                                        "text": "路径与规则绑定",
                                    }
                                ],
                            },
                            {"component": "VDivider"},
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "directory_rules",
                                                            "label": "目录绑定规则集",
                                                            "rows": 4,
                                                            "placeholder": _SAMPLE_DIRECTORY_RULES,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "history_context_methods",
                                                            "label": "历史上下文方法(每行一个，兼容旧JSON数组)",
                                                            "rows": 6,
                                                            "placeholder": _SAMPLE_HISTORY_CONTEXT_METHOD_LINES,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "目录规则支持按顺序绑定多个规则集。历史上下文方法现在支持按行填写，"
                                                "旧的 JSON 数组配置仍然兼容。通常先改目录绑定，再按需微调高级规则。"
                                            ),
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"class": "mt-3"},
                        "content": [
                            {
                                "component": "VCardTitle",
                                "props": {"class": "d-flex align-center"},
                                "content": [
                                    {
                                        "component": "span",
                                        "text": "高级规则配置",
                                    }
                                ],
                            },
                            {"component": "VDivider"},
                            {
                                "component": "VCardText",
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "cleanup_profiles",
                                                            "label": "清洗配置(JSON)",
                                                            "rows": 18,
                                                            "placeholder": _SAMPLE_CLEANUP_PROFILES,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "rule_sets",
                                                            "label": "规则集(JSON)",
                                                            "rows": 16,
                                                            "placeholder": _SAMPLE_RULE_SETS,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "overrides",
                                                            "label": "手工覆盖(JSON数组)",
                                                            "rows": 10,
                                                            "placeholder": _SAMPLE_OVERRIDES,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "variety_episode_mappings",
                                                            "label": "综艺集号映射(JSON数组)",
                                                            "rows": 16,
                                                            "placeholder": _SAMPLE_VARIETY_EPISODE_MAPPINGS,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": (
                                                "高级区保留完整 JSON 配置能力。规则可通过 filename_fallback "
                                                "控制是否参与无路径兜底；override 支持 contains/equals/regex，"
                                                "high_risk_mode 可配置 warn/skip；综艺集号映射默认同时提供“现在就出发3”和一份可复制修改的通用模板。"
                                            ),
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "hook_dirmonitor": True,
            "hook_recognize_media": True,
            "only_strm": True,
            "config_revision": _CONFIG_REVISION,
            "directory_rules": _SAMPLE_DIRECTORY_RULES,
            "rule_sets": _SAMPLE_RULE_SETS,
            "cleanup_profiles": _SAMPLE_CLEANUP_PROFILES,
            "overrides": "[]",
            "variety_episode_mappings": _SAMPLE_VARIETY_EPISODE_MAPPINGS,
            "history_context_methods": _SAMPLE_HISTORY_CONTEXT_METHOD_LINES,
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        try:
            from app.chain import ChainBase
        except Exception:
            ChainBase = None

        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
        except Exception:
            DownloadHistoryOper = None

        try:
            from app.db.transferhistory_oper import TransferHistoryOper
        except Exception:
            TransferHistoryOper = None

        for module_name, (module, original_metainfo) in list(self._original_metainfo_targets.items()):
            current_metainfo = getattr(module, "MetaInfoPath", None)
            if getattr(current_metainfo, "_patched_by", object()) == id(self):
                setattr(module, "MetaInfoPath", original_metainfo)
        self._original_metainfo_targets = {}

        if (
            DownloadHistoryOper
            and self._original_downloadhis_get_file_by_fullpath
            and getattr(DownloadHistoryOper.get_file_by_fullpath, "_patched_by", object()) == id(self)
        ):
            DownloadHistoryOper.get_file_by_fullpath = self._original_downloadhis_get_file_by_fullpath

        if (
            ChainBase
            and self._original_chain_recognize_media
            and getattr(ChainBase.recognize_media, "_patched_by", object()) == id(self)
        ):
            ChainBase.recognize_media = self._original_chain_recognize_media

        if (
            ChainBase
            and self._original_chain_async_recognize_media
            and getattr(ChainBase.async_recognize_media, "_patched_by", object()) == id(self)
        ):
            ChainBase.async_recognize_media = self._original_chain_async_recognize_media

        if TransferHistoryOper and self._original_transferhis_methods:
            for method_name, original_method in self._original_transferhis_methods.items():
                current_method = getattr(TransferHistoryOper, method_name, None)
                if getattr(current_method, "_patched_by", object()) == id(self):
                    setattr(TransferHistoryOper, method_name, original_method)

        self._clear_history_src_context()
        self._clear_dirmonitor_bypass_context()
