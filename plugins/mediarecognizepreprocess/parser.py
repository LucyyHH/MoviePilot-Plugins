import copy
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from app.core.meta import MetaBase
from app.schemas.types import MediaType

from app.log import logger
from app.plugins.mediarecognizepreprocess.models import (
    CleanupProfile,
    MetaSource,
    OverrideDef,
    ParseResult,
    RuleDef,
    VarietyEpisodeCase,
    VarietyEpisodeMapping,
    VarietyEpisodeTemplate,
)


class MediaRecognizePreprocessParser:
    _DEFAULT_CLEANUP_PROFILE_NAME = "default_movie_cleanup"
    _ALLOWED_HIGH_RISK_MODES = {"apply", "warn", "skip"}
    _ALLOWED_OVERRIDE_MATCH_MODES = {"contains", "equals", "regex"}
    _ALLOWED_OVERRIDE_MATCH_ON = {"raw_name", "source_path", "both"}
    _URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
    _LOW_SIGNAL_TECH_TOKENS = {
        "aac",
        "ac3",
        "atmos",
        "avc",
        "bd",
        "bdrip",
        "bluray",
        "dd",
        "ddp",
        "dl",
        "dv",
        "dovi",
        "dts",
        "eac3",
        "flac",
        "h264",
        "h265",
        "hdr",
        "hdr10",
        "hevc",
        "multi",
        "remux",
        "truehd",
        "uhd",
        "web",
        "webdl",
        "x264",
        "x265",
    }
    _PATH_ATTRS = (
        "src_path",
        "source_path",
        "fullpath",
        "file_path",
        "path",
        "filepath",
        "original_path",
        "origin_path",
        "source_file",
        "torrent_path",
    )
    _RAW_ATTRS = (
        "org_string",
        "org_name",
        "org_title",
        "original_name",
        "raw_name",
        "raw_title",
        "subtitle",
        "title",
        "name",
        "cn_name",
        "en_name",
    )
    _KNOWN_SUFFIXES = (".strm", ".mkv", ".mp4", ".avi", ".ts", ".iso")
    _YEAR_PREFIX_PATTERN = re.compile(r"^(19|20)\d{2}[.\s_-]+.+$")
    _TIME_NAME_PATTERN = re.compile(r"^\d{2}[:：]\d{2}$")

    def __init__(
        self,
        directory_rules_text: str,
        rule_sets_text: str,
        cleanup_profiles_text: str,
        overrides_text: str,
        variety_episode_mappings_text: str = "[]",
        only_strm: bool = True,
    ):
        self.only_strm = bool(only_strm)
        self.directory_rules = self._parse_directory_rules(directory_rules_text)
        self.cleanup_profiles = self._parse_cleanup_profiles(cleanup_profiles_text)
        self.rule_sets = self._parse_rule_sets(rule_sets_text)
        self.overrides = self._parse_overrides(overrides_text)
        self.variety_episode_mappings = self._parse_variety_episode_mappings(variety_episode_mappings_text)
        self._build_runtime_caches()
        self._validate_config()

    def match(
        self,
        meta: MetaBase,
        log_misses: bool = True,
    ) -> Tuple[Optional[ParseResult], Optional[MetaSource]]:
        source = self.extract_meta_source(meta)
        return self._match_source(source=source, meta=meta, log_misses=log_misses)

    def match_path(
        self,
        file_path: Path,
        log_misses: bool = True,
    ) -> Tuple[Optional[ParseResult], Optional[MetaSource]]:
        source = MetaSource(
            source_path=self._normalize_dir(str(file_path)),
            raw_name=file_path.stem,
            current_name=file_path.stem,
        )
        return self._match_source(source=source, meta=None, log_misses=log_misses)

    def should_bypass_download_history(self, file_path: Path) -> bool:
        result, _ = self.match_path(file_path, log_misses=False)
        return bool(result)

    def _match_source(
        self,
        source: Optional[MetaSource],
        meta: Optional[MetaBase] = None,
        log_misses: bool = True,
    ) -> Tuple[Optional[ParseResult], Optional[MetaSource]]:
        if not source or not source.raw_name:
            if meta is not None and log_misses:
                logger.debug(
                    f"媒体识别预处理未提取到原始名称，meta快照：{self._debug_meta_snapshot(meta)}"
                )
            return None, source

        ruleset_names = self._match_rulesets(source.source_path)
        if self.directory_rules and not ruleset_names:
            if not source.source_path:
                return self._match_rules_without_path(source, log_misses=log_misses)
            if log_misses:
                logger.debug(
                    f"媒体识别预处理未命中目录规则：source_path={source.source_path}, "
                    f"raw_name={source.raw_name}"
                )
            return None, source

        if self.only_strm and source.source_path:
            suffix = Path(source.source_path).suffix.lower()
            if suffix != ".strm":
                if log_misses:
                    logger.debug(
                        f"媒体识别预处理跳过非STRM文件：source_path={source.source_path}, suffix={suffix}"
                    )
                return None, source

        override = self._match_override(source)
        if override:
            result = ParseResult(
                title=override.title,
                year=override.year,
                media_type=override.media_type,
                tmdbid=override.tmdbid,
                season=override.season,
                episode=override.episode,
                matched_by="override",
                matched_rule=override.keyword,
            )
            result = self._apply_high_risk_strategy(
                source=source,
                result=result,
                strategy=override.high_risk_mode,
                context=f"override={override.keyword}",
            )
            if result:
                result = self._apply_variety_episode_mapping(source, result)
                logger.info(
                    f"媒体识别预处理命中覆盖规则：keyword={override.keyword}, "
                    f"title={result.title}, year={result.year}, raw_name={source.raw_name}"
                )
                return result, source

        if not ruleset_names:
            if not source.source_path:
                return self._match_rules_without_path(source, log_misses=log_misses)
            if log_misses:
                logger.debug(
                    f"媒体识别预处理未找到可用规则集：source_path={source.source_path}, raw_name={source.raw_name}"
                )
            return None, source

        for ruleset_name in ruleset_names:
            for rule in self.rule_sets.get(ruleset_name, []):
                result = self._apply_rule(source, rule)
                if result:
                    result = self._apply_high_risk_strategy(
                        source=source,
                        result=result,
                        strategy=rule.high_risk_mode,
                        context=f"ruleset={ruleset_name}, rule={rule.name}",
                    )
                    if result:
                        result = self._apply_variety_episode_mapping(source, result)
                        logger.info(
                            f"媒体识别预处理命中规则：ruleset={ruleset_name}, rule={rule.name}, "
                            f"title={result.title}, year={result.year}, raw_name={source.raw_name}"
                        )
                        return result, source
        if log_misses:
            logger.debug(
                f"媒体识别预处理规则集未匹配：rulesets={ruleset_names}, raw_name={source.raw_name}"
            )
        return None, source

    def _match_rules_without_path(
        self,
        source: MetaSource,
        log_misses: bool = True,
    ) -> Tuple[Optional[ParseResult], Optional[MetaSource]]:
        for ruleset_name, rules in self.rule_sets.items():
            for rule in rules:
                if rule.use_path or not rule.filename_fallback:
                    continue
                result = self._apply_rule(source, rule)
                if not result:
                    continue
                result = self._apply_high_risk_strategy(
                    source=source,
                    result=result,
                    strategy=rule.high_risk_mode,
                    context=f"ruleset={ruleset_name}, rule={rule.name}, mode=filename_fallback",
                )
                if not result:
                    continue
                result.matched_by = "filename_fallback"
                result = self._apply_variety_episode_mapping(source, result)
                logger.info(
                    f"媒体识别预处理命中无路径兜底规则：ruleset={ruleset_name}, rule={rule.name}, "
                    f"title={result.title}, year={result.year}, raw_name={source.raw_name}"
                )
                return result, source

        if log_misses:
            logger.debug(
                f"媒体识别预处理无路径兜底未匹配：raw_name={source.raw_name}, current_name={source.current_name}"
            )
        return None, source

    def apply(self, meta: MetaBase, result: ParseResult, fallback_type: Optional[MediaType] = None) -> MetaBase:
        try:
            new_meta = copy.deepcopy(meta)
        except Exception:
            new_meta = copy.copy(meta)

        normalized_type = self._normalize_media_type(result.media_type, fallback_type)
        if normalized_type:
            setattr(new_meta, "type", normalized_type)

        for attr in ("title", "name", "cn_name", "en_name"):
            try:
                setattr(new_meta, attr, result.title)
            except Exception:
                continue

        if result.year:
            try:
                setattr(new_meta, "year", str(result.year))
            except Exception:
                pass

        if result.season is not None:
            try:
                setattr(new_meta, "begin_season", int(result.season))
            except Exception:
                pass

        if result.episode is not None:
            try:
                setattr(new_meta, "begin_episode", int(result.episode))
            except Exception:
                pass

        if result.tmdbid:
            for attr in ("tmdbid", "tmdb_id"):
                try:
                    setattr(new_meta, attr, int(result.tmdbid))
                except Exception:
                    continue

        return new_meta

    def extract_meta_source(self, meta: MetaBase) -> Optional[MetaSource]:
        source_path = None
        raw_name = None

        for attr in self._PATH_ATTRS:
            value = getattr(meta, attr, None)
            normalized_path = self._normalize_path(value)
            if not normalized_path:
                continue
            source_path = normalized_path
            raw_name = Path(normalized_path).stem
            break

        for attr in self._RAW_ATTRS:
            value = self._normalize_text(getattr(meta, attr, None))
            if not value:
                continue
            if not raw_name:
                if self._looks_like_path(value):
                    normalized_path = self._normalize_path(value)
                    if normalized_path:
                        source_path = source_path or normalized_path
                        raw_name = Path(normalized_path).stem
                        continue
                raw_name = self._extract_name(value)
            break

        current_name = self._normalize_text(getattr(meta, "name", None) or getattr(meta, "title", None))
        if not raw_name:
            return None
        return MetaSource(
            source_path=source_path,
            raw_name=raw_name,
            current_name=current_name,
        )

    def _match_rulesets(self, source_path: Optional[str]) -> List[str]:
        if not self.directory_rules:
            return []
        if not source_path:
            return []

        normalized_source = self._normalize_dir(source_path)
        matched_dir = None
        matched_rulesets: List[str] = []
        for directory, rulesets in self.directory_rules.items():
            if normalized_source == directory or normalized_source.startswith(f"{directory}/"):
                if matched_dir is None or len(directory) > len(matched_dir):
                    matched_dir = directory
                    matched_rulesets = rulesets
        return matched_rulesets

    def _match_override(self, source: MetaSource) -> Optional[OverrideDef]:
        for item in self.overrides:
            if item.directories:
                if not source.source_path:
                    continue
                normalized_source = self._normalize_dir(source.source_path)
                matched = False
                for directory in item.directories:
                    normalized_dir = self._normalize_dir(directory)
                    if normalized_source == normalized_dir or normalized_source.startswith(f"{normalized_dir}/"):
                        matched = True
                        break
                if not matched:
                    continue
            if self._override_matches(source, item):
                return item
        return None

    def _override_matches(self, source: MetaSource, item: OverrideDef) -> bool:
        match_on = item.match_on if item.match_on in self._ALLOWED_OVERRIDE_MATCH_ON else "both"
        match_mode = item.match_mode if item.match_mode in self._ALLOWED_OVERRIDE_MATCH_MODES else "contains"
        targets: List[str] = []
        if match_on in ("raw_name", "both"):
            targets.append(source.raw_name or "")
        if match_on in ("source_path", "both"):
            targets.append(source.source_path or "")

        if match_mode == "regex":
            if not item.compiled_pattern:
                return False
            return any(item.compiled_pattern.search(text or "") for text in targets)

        keyword = item.keyword if item.case_sensitive else item.keyword.lower()
        for text in targets:
            candidate = text or ""
            if not item.case_sensitive:
                candidate = candidate.lower()
            if match_mode == "equals" and candidate == keyword:
                return True
            if match_mode == "contains" and keyword in candidate:
                return True
        return False

    def _apply_rule(self, source: MetaSource, rule: RuleDef) -> Optional[ParseResult]:
        target_text = source.raw_name
        if rule.use_path and source.source_path:
            target_text = self._path_without_suffix(source.source_path)
        if not rule.compiled_pattern:
            return None
        match = rule.compiled_pattern.search(target_text)

        if not match:
            return None

        title = self._group_value(match, rule.title_group)
        if not title:
            return None

        title = self._clean_title(title, rule)
        if not title:
            return None

        year = self._group_value(match, rule.year_group)
        season = self._group_value(match, rule.season_group)
        episode = self._group_value(match, rule.episode_group)

        return ParseResult(
            title=title,
            year=year,
            media_type=rule.media_type,
            season=self._to_int(season),
            episode=self._to_int(episode),
            matched_by="rule",
            matched_rule=rule.name,
        )

    def _apply_variety_episode_mapping(self, source: MetaSource, result: ParseResult) -> ParseResult:
        mapping = self._match_variety_mapping(source, result)
        if not mapping or not mapping.compiled_pattern:
            return result

        tokens = self._extract_variety_tokens(source, mapping)
        if not tokens:
            return result

        fixed_case = self._find_variety_case(mapping.special_fixed, tokens, source)
        if fixed_case and fixed_case.episode is not None:
            return self._build_variety_result(result, mapping, fixed_case.episode, season=fixed_case.season)

        insert_case = self._find_variety_case(mapping.special_inserts, tokens, source)
        if insert_case and insert_case.episode is not None:
            return self._build_variety_result(result, mapping, insert_case.episode, season=insert_case.season)

        base_episode = self._resolve_variety_template_episode(mapping, tokens.get("issue"), tokens.get("part_tag"))
        if base_episode is None:
            return result

        final_episode = self._apply_variety_insert_shifts(base_episode, mapping.special_inserts)
        return self._build_variety_result(result, mapping, final_episode)

    def _build_variety_result(
        self,
        result: ParseResult,
        mapping: VarietyEpisodeMapping,
        episode: int,
        season: Optional[int] = None,
    ) -> ParseResult:
        rewritten = copy.copy(result)
        rewritten.episode = int(episode)
        resolved_season = season if season is not None else mapping.season
        if resolved_season is not None:
            rewritten.season = int(resolved_season)
        rewritten.matched_by = f"{result.matched_by}+variety" if result.matched_by else "variety"
        mapping_name = mapping.name or "variety_episode_mapping"
        rewritten.matched_rule = (
            f"{result.matched_rule}|{mapping_name}" if result.matched_rule else mapping_name
        )
        return rewritten

    def _match_variety_mapping(
        self,
        source: MetaSource,
        result: ParseResult,
    ) -> Optional[VarietyEpisodeMapping]:
        normalized_title = self._normalize_name(result.title or "")
        normalized_path = self._normalize_dir(source.source_path) if source.source_path else ""
        current_tmdbid = self._to_int(result.tmdbid)

        for mapping in self.variety_episode_mappings:
            if mapping.tmdbid and current_tmdbid and mapping.tmdbid != current_tmdbid:
                continue
            if mapping.directories:
                if not normalized_path:
                    continue
                matched_directory = False
                for directory in mapping.directories:
                    normalized_directory = self._normalize_dir(directory)
                    if normalized_path == normalized_directory or normalized_path.startswith(f"{normalized_directory}/"):
                        matched_directory = True
                        break
                if not matched_directory:
                    continue
            if mapping.tmdbid and current_tmdbid and mapping.tmdbid == current_tmdbid:
                return mapping
            if mapping.title_keywords:
                matched_keyword = False
                for keyword in mapping.title_keywords:
                    normalized_keyword = self._normalize_name(keyword)
                    if normalized_keyword and normalized_keyword in normalized_title:
                        matched_keyword = True
                        break
                if not matched_keyword:
                    continue
                return mapping
            if not mapping.tmdbid and not mapping.title_keywords and mapping.directories:
                return mapping
        return None

    def _extract_variety_tokens(
        self,
        source: MetaSource,
        mapping: VarietyEpisodeMapping,
    ) -> Optional[Dict[str, Optional[object]]]:
        candidates = [source.raw_name]
        if source.source_path:
            candidates.append(Path(source.source_path).stem)
        if source.current_name:
            candidates.append(source.current_name)

        for candidate in candidates:
            if not candidate:
                continue
            match = mapping.compiled_pattern.search(candidate)
            if not match:
                continue
            return {
                "date": self._group_value(match, mapping.date_group),
                "issue": self._to_int(self._group_value(match, mapping.issue_group)),
                "part_tag": self._normalize_variety_part_tag(self._group_value(match, mapping.part_tag_group)),
            }
        return None

    def _resolve_variety_template_episode(
        self,
        mapping: VarietyEpisodeMapping,
        issue: Optional[int],
        part_tag: Optional[str],
    ) -> Optional[int]:
        template = mapping.template
        if not template or issue is None or not part_tag:
            return None
        offset = template.part_offsets.get(part_tag)
        if offset is None:
            return None
        return int(template.start_episode) + max(int(issue) - 1, 0) * int(template.issue_step) + int(offset)

    def _apply_variety_insert_shifts(
        self,
        base_episode: int,
        insert_cases: List[VarietyEpisodeCase],
    ) -> int:
        candidate = int(base_episode)
        while True:
            shift_count = 0
            for case in insert_cases:
                if not case.shift_following or case.episode is None:
                    continue
                if int(case.episode) <= candidate:
                    shift_count += 1
            shifted = int(base_episode) + shift_count
            if shifted == candidate:
                return shifted
            candidate = shifted

    def _find_variety_case(
        self,
        cases: List[VarietyEpisodeCase],
        tokens: Dict[str, Optional[object]],
        source: MetaSource,
    ) -> Optional[VarietyEpisodeCase]:
        for case in cases:
            if self._variety_case_matches(case, tokens, source):
                return case
        return None

    def _variety_case_matches(
        self,
        case: VarietyEpisodeCase,
        tokens: Dict[str, Optional[object]],
        source: MetaSource,
    ) -> bool:
        identifier_required = bool(case.date or case.title_keywords)
        identifier_matched = False
        if case.date and str(case.date) == str(tokens.get("date") or ""):
            identifier_matched = True
        if case.title_keywords and self._variety_case_title_matches(case, source):
            identifier_matched = True
        if identifier_required and not identifier_matched:
            return False
        if case.issue is not None and int(case.issue) != int(tokens.get("issue") or -1):
            return False
        if case.part_tag and self._normalize_variety_part_tag(case.part_tag) != self._normalize_variety_part_tag(tokens.get("part_tag")):
            return False
        return True

    def _variety_case_title_matches(self, case: VarietyEpisodeCase, source: MetaSource) -> bool:
        if not case.title_keywords:
            return False
        candidates: List[str] = [source.raw_name or "", source.current_name or ""]
        if source.source_path:
            candidates.append(Path(source.source_path).stem)
            candidates.append(source.source_path)
        normalized_candidates = [self._normalize_name(item) for item in candidates if item]
        for keyword in case.title_keywords:
            normalized_keyword = self._normalize_name(keyword)
            if not normalized_keyword:
                continue
            if any(normalized_keyword in candidate for candidate in normalized_candidates):
                return True
        return False

    @staticmethod
    def _normalize_variety_part_tag(value: Optional[object]) -> Optional[str]:
        if value in (None, ""):
            return None
        normalized = str(value).strip().replace("：", ":")
        normalized = re.sub(r"\s+", "", normalized)
        return normalized or None

    def _clean_title(self, title: str, rule: RuleDef) -> str:
        cleaned = title
        for pattern in rule.compiled_strip_patterns:
            cleaned = pattern.sub(" ", cleaned)
        profile = self._get_cleanup_profile(rule.cleanup_profile)
        for pattern in profile.compiled_strip_patterns:
            cleaned = pattern.sub(" ", cleaned)
        cleaned = self._normalize_title_text(cleaned, profile)
        cleaned = cleaned.strip(" -._")
        if not cleaned:
            return ""
        if self._is_low_signal_title(cleaned):
            return ""
        return cleaned

    def _normalize_title_text(self, text: str, profile: CleanupProfile) -> str:
        cleaned = text
        for pattern in profile.compiled_bracket_patterns:
            cleaned = pattern.sub(" ", cleaned)

        if profile.strip_urls:
            cleaned = self._URL_PATTERN.sub(" ", cleaned)

        cleaned = re.sub(r"[._]+", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        previous = None
        while cleaned and cleaned != previous:
            previous = cleaned
            if profile.compiled_release_pattern:
                cleaned = profile.compiled_release_pattern.sub(" ", cleaned)
            if profile.compiled_language_pattern:
                cleaned = profile.compiled_language_pattern.sub(" ", cleaned)
            if profile.compiled_subtitle_pattern:
                cleaned = profile.compiled_subtitle_pattern.sub(" ", cleaned)
            if profile.compiled_audio_pattern:
                cleaned = profile.compiled_audio_pattern.sub(" ", cleaned)
            if profile.compiled_misc_pattern:
                cleaned = profile.compiled_misc_pattern.sub(" ", cleaned)
            if profile.compiled_trailing_tag_pattern:
                cleaned = profile.compiled_trailing_tag_pattern.sub("", cleaned)
            for pattern in profile.compiled_trailing_patterns:
                cleaned = pattern.sub("", cleaned)
            cleaned = re.sub(r"\s+", " ", cleaned).strip(" -._")

        return cleaned

    def _is_low_signal_title(self, title: str) -> bool:
        normalized = re.sub(r"[\s._-]+", " ", str(title or "").strip().lower())
        normalized = normalized.replace("：", ":")
        if not normalized:
            return True
        if re.fullmatch(r"\d{3,4}(?:p|k)", normalized):
            return True
        if re.fullmatch(r"\d+(?::\d+)+", normalized):
            return True

        tokens = [token for token in re.split(r"[\s:]+", normalized) if token]
        if not tokens:
            return True
        if len(tokens) > 1 and all(token.isdigit() for token in tokens):
            return True

        def _is_tech_token(token: str) -> bool:
            compact = token.replace("-", "").replace(".", "")
            if compact in self._LOW_SIGNAL_TECH_TOKENS:
                return True
            if re.fullmatch(r"\d{3,4}(?:p|k)", token):
                return True
            if re.fullmatch(r"\d+(?:\.\d+)?", token):
                return True
            return False

        return all(_is_tech_token(token) for token in tokens)

    def _get_cleanup_profile(self, cleanup_profile_name: Optional[str]) -> CleanupProfile:
        profile_name = cleanup_profile_name or self._DEFAULT_CLEANUP_PROFILE_NAME
        profile = self.cleanup_profiles.get(profile_name)
        if profile:
            return profile

        if cleanup_profile_name:
            logger.warning(
                f"媒体识别预处理清洗配置不存在，回退默认profile：requested={cleanup_profile_name}, "
                f"fallback={self._DEFAULT_CLEANUP_PROFILE_NAME}"
            )

        fallback = self.cleanup_profiles.get(self._DEFAULT_CLEANUP_PROFILE_NAME)
        if fallback:
            return fallback

        return CleanupProfile(strip_urls=True)

    def _build_token_pattern(self, tokens: List[str]) -> Optional[str]:
        parts = [self._token_to_pattern(token) for token in tokens if str(token).strip()]
        if not parts:
            return None
        return r"(?<![0-9a-z])(?:%s)(?![0-9a-z])" % "|".join(parts)

    def _build_token_block_pattern(self, tokens: List[str]) -> Optional[str]:
        token_pattern = self._build_token_pattern(tokens)
        if not token_pattern:
            return None
        inner = token_pattern.replace("(?<![0-9a-z])", "").replace("(?![0-9a-z])", "")
        return r"(?<![0-9a-z])(?:%s)(?:[-_. ]*(?:%s))*(?![0-9a-z])" % (inner, inner)

    def _build_trailing_tag_pattern(self, profile: CleanupProfile) -> Optional[str]:
        groups = []
        for tokens in (
            profile.release_tags,
            profile.language_tags,
            profile.subtitle_tags,
            profile.audio_tags,
            profile.misc_tags,
        ):
            token_pattern = self._build_token_pattern(tokens)
            if token_pattern:
                groups.append(token_pattern.replace("(?<![0-9a-z])", "").replace("(?![0-9a-z])", ""))
        if not groups:
            return None
        return r"(?:[\s._-]*(?:%s))+$" % "|".join(groups)

    @staticmethod
    def _token_to_pattern(token: str) -> str:
        escaped = re.escape(str(token).strip())
        return re.sub(r"(\\[-_. ])+", r"[-_. ]*", escaped)

    def _is_high_risk(self, source: MetaSource, result: ParseResult) -> bool:
        current_name = source.current_name or ""
        if self._TIME_NAME_PATTERN.match(current_name):
            return True
        if self._YEAR_PREFIX_PATTERN.match(source.raw_name):
            return True
        return current_name and self._normalize_name(current_name) != self._normalize_name(result.title)

    def _apply_high_risk_strategy(
        self,
        source: MetaSource,
        result: ParseResult,
        strategy: str,
        context: str,
    ) -> Optional[ParseResult]:
        result.high_risk = self._is_high_risk(source, result)
        if not result.high_risk:
            return result

        normalized_strategy = strategy if strategy in self._ALLOWED_HIGH_RISK_MODES else "apply"
        if normalized_strategy == "skip":
            logger.warning(
                f"媒体识别预处理高风险命中已跳过：{context}, title={result.title}, "
                f"year={result.year}, raw_name={source.raw_name}"
            )
            return None
        if normalized_strategy == "warn":
            logger.warning(
                f"媒体识别预处理高风险命中：{context}, title={result.title}, "
                f"year={result.year}, raw_name={source.raw_name}"
            )
        return result

    @staticmethod
    def _group_value(match: re.Match, group_name: Optional[str]) -> Optional[str]:
        if not group_name:
            return None
        try:
            value = match.group(group_name)
        except IndexError:
            return None
        except KeyError:
            return None
        if value is None:
            return None
        return str(value).strip()

    @staticmethod
    def _to_int(value: Optional[str]) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_text(value) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _normalize_path(self, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, Path):
            return self._normalize_dir(str(value))
        text = str(value).strip()
        if not text:
            return None
        if not self._looks_like_path(text):
            return None
        return self._normalize_dir(text)

    @staticmethod
    def _normalize_dir(path: str) -> str:
        normalized = path.replace("\\", "/").strip()
        return normalized.rstrip("/") or "/"

    def _extract_name(self, value: str) -> str:
        if self._looks_like_path(value):
            normalized_path = self._normalize_path(value)
            if normalized_path:
                return Path(normalized_path).stem
        return value

    def _looks_like_path(self, value: str) -> bool:
        lowered = value.lower()
        return (
            "/" in value
            or "\\" in value
            or lowered.endswith(self._KNOWN_SUFFIXES)
        )

    @staticmethod
    def _normalize_name(value: str) -> str:
        normalized = value.lower().replace("：", ":")
        normalized = re.sub(r"[\s._-]+", "", normalized)
        return normalized

    @staticmethod
    def _normalize_media_type(media_type: Optional[str], fallback: Optional[MediaType]) -> Optional[MediaType]:
        if isinstance(media_type, MediaType):
            return media_type
        lowered = (media_type or "").lower()
        if lowered in ("movie", "film"):
            return MediaType.MOVIE
        if lowered in ("tv", "show", "series"):
            return MediaType.TV
        return fallback

    def _parse_directory_rules(self, text: str) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for raw_line in (text or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=>" not in line:
                logger.warning(f"媒体识别预处理目录规则格式无效，已忽略：{line}")
                continue
            directory, ruleset_text = [part.strip() for part in line.split("=>", 1)]
            rulesets = [item.strip() for item in ruleset_text.split(",") if item.strip()]
            if not directory or not rulesets:
                continue
            result[self._normalize_dir(directory)] = rulesets
        return result

    def _parse_cleanup_profiles(self, text: str) -> Dict[str, CleanupProfile]:
        if not text:
            return {}

        try:
            raw = json.loads(text)
        except json.JSONDecodeError as err:
            logger.error(f"媒体识别预处理清洗配置JSON解析失败：{err}")
            return {}

        if not isinstance(raw, dict):
            logger.error("媒体识别预处理清洗配置必须是JSON对象")
            return {}

        result: Dict[str, CleanupProfile] = {}
        for profile_name, item in raw.items():
            if not isinstance(item, dict):
                continue
            result[str(profile_name)] = CleanupProfile(
                strip_patterns=self._coerce_list(item.get("strip_patterns")),
                release_tags=self._coerce_list(item.get("release_tags")),
                language_tags=self._coerce_list(item.get("language_tags")),
                subtitle_tags=self._coerce_list(item.get("subtitle_tags")),
                audio_tags=self._coerce_list(item.get("audio_tags")),
                misc_tags=self._coerce_list(item.get("misc_tags")),
                trailing_patterns=self._coerce_list(item.get("trailing_patterns")),
                bracket_patterns=self._coerce_list(item.get("bracket_patterns")),
                strip_urls=self._coerce_bool(item.get("strip_urls"), True),
            )

        return result

    def _build_runtime_caches(self) -> None:
        for profile_name, profile in self.cleanup_profiles.items():
            profile.compiled_strip_patterns = self._compile_pattern_list(
                profile.strip_patterns,
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, strip_patterns",
            )
            profile.compiled_bracket_patterns = self._compile_pattern_list(
                profile.bracket_patterns,
                context=f"cleanup_profile={profile_name}, bracket_patterns",
            )
            profile.compiled_trailing_patterns = self._compile_pattern_list(
                profile.trailing_patterns,
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, trailing_patterns",
            )
            profile.compiled_release_pattern = self._compile_single_pattern(
                self._build_token_block_pattern(profile.release_tags),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, release_tags",
            )
            profile.compiled_language_pattern = self._compile_single_pattern(
                self._build_token_pattern(profile.language_tags),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, language_tags",
            )
            profile.compiled_subtitle_pattern = self._compile_single_pattern(
                self._build_token_pattern(profile.subtitle_tags),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, subtitle_tags",
            )
            profile.compiled_audio_pattern = self._compile_single_pattern(
                self._build_token_pattern(profile.audio_tags),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, audio_tags",
            )
            profile.compiled_misc_pattern = self._compile_single_pattern(
                self._build_token_pattern(profile.misc_tags),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, misc_tags",
            )
            profile.compiled_trailing_tag_pattern = self._compile_single_pattern(
                self._build_trailing_tag_pattern(profile),
                flags=re.IGNORECASE,
                context=f"cleanup_profile={profile_name}, trailing_tag",
            )

        for rules in self.rule_sets.values():
            for rule in rules:
                rule.compiled_pattern = self._compile_single_pattern(
                    rule.pattern,
                    flags=re.IGNORECASE,
                    context=f"rule={rule.name}",
                )
                rule.compiled_strip_patterns = self._compile_pattern_list(
                    rule.strip_patterns,
                    flags=re.IGNORECASE,
                    context=f"rule={rule.name}, strip_patterns",
                )

        for override in self.overrides:
            if override.match_mode == "regex":
                flags = 0 if override.case_sensitive else re.IGNORECASE
                override.compiled_pattern = self._compile_single_pattern(
                    override.keyword,
                    flags=flags,
                    context=f"override={override.keyword}",
                )

        for mapping in self.variety_episode_mappings:
            mapping.compiled_pattern = self._compile_single_pattern(
                mapping.pattern,
                flags=re.IGNORECASE,
                context=f"variety_mapping={mapping.name}",
            )

    @staticmethod
    def _compile_pattern_list(patterns: List[str], flags: int = 0, context: str = "") -> List[re.Pattern]:
        compiled: List[re.Pattern] = []
        for pattern in patterns:
            compiled_pattern = MediaRecognizePreprocessParser._compile_single_pattern(pattern, flags=flags, context=context)
            if compiled_pattern:
                compiled.append(compiled_pattern)
        return compiled

    @staticmethod
    def _compile_single_pattern(
        pattern: Optional[str],
        flags: int = 0,
        context: str = "",
    ) -> Optional[re.Pattern]:
        if not pattern:
            return None
        try:
            return re.compile(pattern, flags)
        except re.error as err:
            logger.error(f"媒体识别预处理正则无效：context={context}, pattern={pattern}, error={err}")
            return None

    def _parse_rule_sets(self, text: str) -> Dict[str, List[RuleDef]]:
        if not text:
            return {}
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as err:
            logger.error(f"媒体识别预处理规则集JSON解析失败：{err}")
            return {}

        result: Dict[str, List[RuleDef]] = {}
        if not isinstance(raw, dict):
            logger.error("媒体识别预处理规则集必须是JSON对象")
            return result

        for ruleset_name, items in raw.items():
            if isinstance(items, dict):
                items = [items]
            if not isinstance(items, list):
                continue
            parsed_rules: List[RuleDef] = []
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                pattern = item.get("pattern")
                if not pattern:
                    continue
                parsed_rules.append(
                    RuleDef(
                        name=item.get("name") or f"{ruleset_name}-{index + 1}",
                        pattern=pattern,
                        media_type=item.get("type") or item.get("media_type") or "movie",
                        use_path=bool(item.get("use_path")),
                        filename_fallback=self._coerce_bool(
                            item.get("filename_fallback"),
                            not bool(item.get("use_path")),
                        ),
                        high_risk_mode=str(item.get("high_risk_mode") or "apply").strip().lower(),
                        cleanup_profile=item.get("cleanup_profile"),
                        title_group=item.get("title_group") or "title",
                        year_group=item.get("year_group", "year"),
                        season_group=item.get("season_group"),
                        episode_group=item.get("episode_group"),
                        strip_patterns=self._coerce_list(item.get("strip_patterns")),
                    )
                )
            if parsed_rules:
                result[ruleset_name] = parsed_rules
        return result

    def _parse_overrides(self, text: str) -> List[OverrideDef]:
        if not text:
            return []
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as err:
            logger.error(f"媒体识别预处理覆盖规则JSON解析失败：{err}")
            return []

        if not isinstance(raw, list):
            logger.error("媒体识别预处理覆盖规则必须是JSON数组")
            return []

        result: List[OverrideDef] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            keyword = item.get("keyword")
            title = item.get("title")
            if not keyword or not title:
                continue
            result.append(
                OverrideDef(
                    keyword=str(keyword),
                    title=str(title),
                    year=str(item.get("year")) if item.get("year") else None,
                    media_type=item.get("type") or item.get("media_type") or "movie",
                    tmdbid=self._to_int(item.get("tmdbid")),
                    season=self._to_int(item.get("season")),
                    episode=self._to_int(item.get("episode")),
                    directories=self._coerce_list(item.get("directories")),
                    match_mode=str(item.get("match_mode") or "contains").strip().lower(),
                    match_on=str(item.get("match_on") or "both").strip().lower(),
                    case_sensitive=self._coerce_bool(item.get("case_sensitive"), False),
                    high_risk_mode=str(item.get("high_risk_mode") or "apply").strip().lower(),
                )
            )
        return result

    def _parse_variety_episode_mappings(self, text: str) -> List[VarietyEpisodeMapping]:
        if not text:
            return []
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as err:
            logger.error(f"媒体识别预处理综艺集号映射JSON解析失败：{err}")
            return []

        if not isinstance(raw, list):
            logger.error("媒体识别预处理综艺集号映射必须是JSON数组")
            return []

        result: List[VarietyEpisodeMapping] = []
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            pattern = item.get("pattern")
            if not pattern:
                continue
            template = None
            template_item = item.get("template")
            if isinstance(template_item, dict):
                template = VarietyEpisodeTemplate(
                    start_episode=self._to_int(template_item.get("start_episode")) or 1,
                    issue_step=self._to_int(template_item.get("issue_step")) or 1,
                    part_offsets=self._coerce_int_dict(template_item.get("part_offsets")),
                )
            result.append(
                VarietyEpisodeMapping(
                    name=str(item.get("name") or f"variety-mapping-{index + 1}"),
                    tmdbid=self._to_int(item.get("tmdbid")),
                    title_keywords=self._coerce_list(item.get("title_keywords")),
                    directories=self._coerce_list(item.get("directories")),
                    season=self._to_int(item.get("season")),
                    pattern=str(pattern),
                    date_group=str(item.get("date_group") or "date"),
                    issue_group=str(item.get("issue_group") or "issue"),
                    part_tag_group=str(item.get("part_tag_group") or "part_tag"),
                    template=template,
                    special_fixed=self._parse_variety_cases(item.get("special_fixed"), shift_following_default=False),
                    special_inserts=self._parse_variety_cases(item.get("special_inserts"), shift_following_default=True),
                )
            )
        return result

    def _parse_variety_cases(self, value, shift_following_default: bool) -> List[VarietyEpisodeCase]:
        if not isinstance(value, list):
            return []
        result: List[VarietyEpisodeCase] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            case = VarietyEpisodeCase(
                date=str(item.get("date")).strip() if item.get("date") not in (None, "") else None,
                issue=self._to_int(item.get("issue")),
                part_tag=self._normalize_variety_part_tag(item.get("part_tag")),
                title_keywords=self._coerce_list(item.get("title_keywords")),
                season=self._to_int(item.get("season")),
                episode=self._to_int(item.get("episode")),
                shift_following=self._coerce_bool(item.get("shift_following"), shift_following_default),
            )
            if case.episode is None or not any((case.date, case.title_keywords, case.issue is not None, case.part_tag)):
                continue
            result.append(case)
        return result

    def _validate_config(self) -> None:
        for directory, rulesets in self.directory_rules.items():
            missing_rulesets = [name for name in rulesets if name not in self.rule_sets]
            if missing_rulesets:
                logger.warning(
                    f"媒体识别预处理目录规则引用了不存在的规则集：directory={directory}, "
                    f"missing_rulesets={missing_rulesets}"
                )

        if self.cleanup_profiles and self._DEFAULT_CLEANUP_PROFILE_NAME not in self.cleanup_profiles:
            logger.warning(
                "媒体识别预处理清洗配置未定义默认profile，缺失profile时将回退到内置最小配置"
            )

        for ruleset_name, rules in self.rule_sets.items():
            for rule in rules:
                if rule.cleanup_profile and rule.cleanup_profile not in self.cleanup_profiles:
                    logger.warning(
                        f"媒体识别预处理规则引用了不存在的清洗配置：ruleset={ruleset_name}, "
                        f"rule={rule.name}, cleanup_profile={rule.cleanup_profile}"
                    )
                if rule.use_path and rule.filename_fallback:
                    logger.warning(
                        f"媒体识别预处理规则同时启用了 use_path 和 filename_fallback："
                        f"ruleset={ruleset_name}, rule={rule.name}"
                    )
                if rule.high_risk_mode not in self._ALLOWED_HIGH_RISK_MODES:
                    logger.warning(
                        f"媒体识别预处理规则 high_risk_mode 无效：ruleset={ruleset_name}, "
                        f"rule={rule.name}, high_risk_mode={rule.high_risk_mode}"
                    )

        for override in self.overrides:
            if override.match_mode not in self._ALLOWED_OVERRIDE_MATCH_MODES:
                logger.warning(
                    f"媒体识别预处理覆盖规则 match_mode 无效：keyword={override.keyword}, "
                    f"match_mode={override.match_mode}"
                )
            if override.match_on not in self._ALLOWED_OVERRIDE_MATCH_ON:
                logger.warning(
                    f"媒体识别预处理覆盖规则 match_on 无效：keyword={override.keyword}, "
                    f"match_on={override.match_on}"
                )
            if override.high_risk_mode not in self._ALLOWED_HIGH_RISK_MODES:
                logger.warning(
                    f"媒体识别预处理覆盖规则 high_risk_mode 无效：keyword={override.keyword}, "
                    f"high_risk_mode={override.high_risk_mode}"
                )

        for mapping in self.variety_episode_mappings:
            if not mapping.tmdbid and not mapping.title_keywords:
                logger.warning(
                    f"媒体识别预处理综艺集号映射缺少节目标识：mapping={mapping.name}"
                )
            if mapping.template:
                if mapping.template.issue_step <= 0:
                    logger.warning(
                        f"媒体识别预处理综艺集号模板步长无效：mapping={mapping.name}, "
                        f"issue_step={mapping.template.issue_step}"
                    )
                if not mapping.template.part_offsets and not mapping.special_fixed and not mapping.special_inserts:
                    logger.warning(
                        f"媒体识别预处理综艺集号映射没有可用模板或特例：mapping={mapping.name}"
                    )
            for case in mapping.special_fixed + mapping.special_inserts:
                if case.episode is None:
                    logger.warning(
                        f"媒体识别预处理综艺集号特例缺少episode：mapping={mapping.name}, case={case}"
                    )

    @staticmethod
    def _coerce_list(value) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [value.strip()] if value.strip() else []
        return []

    @staticmethod
    def _coerce_bool(value, default: bool) -> bool:
        if value is None:
            return default
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
    def _coerce_int_dict(value) -> Dict[str, int]:
        if not isinstance(value, dict):
            return {}
        result: Dict[str, int] = {}
        for key, item in value.items():
            try:
                result[str(key).strip()] = int(item)
            except (TypeError, ValueError):
                continue
        return result

    def _debug_meta_snapshot(self, meta: MetaBase) -> str:
        snapshot = {}
        for attr in self._PATH_ATTRS + self._RAW_ATTRS + ("year", "begin_season", "begin_episode"):
            value = getattr(meta, attr, None)
            if value in (None, ""):
                continue
            text = str(value)
            if len(text) > 120:
                text = text[:117] + "..."
            snapshot[attr] = text
        return json.dumps(snapshot, ensure_ascii=False)

    @staticmethod
    def _path_without_suffix(path: str) -> str:
        normalized = path.replace("\\", "/")
        return re.sub(r"\.[^.\/]+$", "", normalized)
