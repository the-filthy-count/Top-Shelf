# Graph Report - top-shelf  (2026-08-27)

## Corpus Check
- Large corpus: 1682 files · ~1,828,454 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder.

## Summary
- 6742 nodes · 16073 edges · 308 communities (227 shown, 81 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 274 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- Community 17
- Community 18
- Community 19
- Community 20
- Community 21
- Community 22
- Community 23
- Community 25
- Community 26
- Community 27
- Community 29
- Community 30
- Community 31
- Community 32
- Community 33
- Community 34
- Community 35
- Community 36
- Community 37
- Community 38
- Community 39
- Community 40
- Community 41
- Community 42
- Community 43
- Community 44
- Community 45
- Community 46
- Community 47
- Community 48
- Community 49
- Community 50
- Community 51
- Community 52
- Community 53
- Community 54
- Community 55
- Community 56
- Community 58
- Community 59
- Community 60
- Community 61
- Community 62
- Community 63
- Community 64
- Community 65
- Community 66
- Community 67
- Community 68
- Community 69
- Community 70
- Community 71
- Community 72
- Community 73
- Community 74
- Community 75
- Community 76
- Community 77
- Community 78
- Community 79
- Community 80
- Community 81
- Community 82
- Community 83
- Community 84
- Community 85
- Community 86
- Community 87
- Community 88
- Community 89
- Community 90
- Community 91
- Community 92
- Community 93
- Community 94
- Community 95
- Community 96
- Community 97
- Community 98
- Community 99
- Community 100
- Community 101
- Community 102
- Community 103
- Community 104
- Community 105
- Community 106
- Community 107
- Community 108
- Community 109
- Community 110
- Community 111
- Community 112
- Community 113
- Community 114
- Community 115
- Community 116
- Community 117
- Community 118
- Community 119
- Community 120
- Community 121
- Community 122
- Community 123
- Community 124
- Community 125
- Community 126
- Community 127
- Community 128
- Community 129
- Community 130
- Community 131
- Community 132
- Community 133
- Community 134
- Community 135
- Community 136
- Community 137
- Community 138
- Community 139
- Community 140
- Community 141
- Community 142
- Community 143
- Community 144
- Community 145
- Community 146
- Community 147
- Community 148
- Community 149
- Community 150
- Community 151
- Community 152
- Community 153
- Community 154
- Community 155
- Community 156
- Community 157
- Community 158
- Community 159
- Community 160
- Community 161
- Community 162
- Community 163
- Community 164
- Community 165
- Community 166
- Community 167
- Community 168
- Community 169
- Community 170
- Community 171
- Community 172
- Community 173
- Community 174
- Community 175
- Community 176
- Community 177
- Community 178
- Community 179
- Community 180
- Community 181
- Community 182
- Community 183
- Community 184
- Community 185
- Community 186
- Community 187
- Community 188
- Community 189
- Community 190
- Community 191
- Community 192
- Community 193
- Community 194
- Community 195
- Community 196
- Community 197
- Community 198
- Community 199
- Community 200
- Community 201
- Community 202
- Community 203
- Community 204
- Community 205
- Community 206
- Community 207
- Community 208
- Community 209
- Community 210
- Community 211
- Community 212
- Community 213
- Community 214
- Community 215
- Community 216
- Community 217
- Community 218
- Community 219
- Community 220
- Community 221
- Community 222
- Community 223
- Community 224
- Community 225
- Community 226
- Community 227
- Community 228
- Community 229
- Community 230
- Community 231
- Community 232
- Community 233
- Community 234
- Community 235
- Community 236
- Community 237
- Community 238
- Community 239
- Community 240
- Community 241
- Community 242
- Community 243
- Community 244
- Community 245
- Community 246
- Community 247
- Community 248
- Community 249
- Community 250
- Community 251
- Community 252
- Community 253
- Community 254
- Community 255
- Community 256
- Community 257
- Community 258
- Community 259
- Community 260
- Community 261
- Community 262
- Community 263
- Community 264
- Community 265
- Community 266
- Community 267
- Community 268
- Community 269
- Community 270
- Community 271
- Community 272

## God Nodes (most connected - your core abstractions)
1. `get_conn()` - 237 edges
2. `get_settings()` - 186 edges
3. `emit()` - 150 edges
4. `favourite_get()` - 70 edges
5. `renderDetail()` - 45 edges
6. `_tpdb_headers()` - 38 edges
7. `watch()` - 35 edges
8. `run_library_index_job()` - 32 edges
9. `clamp()` - 32 edges
10. `favourite_list()` - 31 edges

## Surprising Connections (you probably didn't know these)
- `queue_file_probe()` --indirect_call--> `get_video_duration()`  [INFERRED]
  main.py → app/core/phash.py
- `queue_file_probe()` --indirect_call--> `probe_video_stream_meta()`  [INFERRED]
  main.py → app/core/phash.py
- `health_library_video_auto_match()` --indirect_call--> `compute_phash()`  [INFERRED]
  main.py → app/core/phash.py
- `health_library_video_phash_match()` --indirect_call--> `compute_phash()`  [INFERRED]
  main.py → app/core/phash.py
- `process_single()` --calls--> `_stashbox_normalize_match_source()`  [EXTRACTED]
  main.py → app/core/filing.py

## Import Cycles
- None detected.

## Communities (308 total, 81 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.01
Nodes (312): _library_index_compute_media_probe_fields(), probe_video_stream_meta(), Stat + optional ffprobe; returns args for library_file_update_media_probe. None…, First video stream: codec name, width, height (via ffprobe)., get, HTMLResponse, api_downloads(), api_downloads_rss_feed() (+304 more)

### Community 1 - "Community 1"
Cohesion: 0.01
Nodes (295): BackgroundTasks, favourite_delete(), get_directories(), get_settings(), Remove a favourites index row. Returns True if a row was deleted., JSONResponse, api_favourites_clear_matches(), api_favourites_delete() (+287 more)

### Community 2 - "Community 2"
Cohesion: 0.02
Nodes (211): favourite_get(), favourite_list(), favourite_set_ext_link(), favourite_update_matches(), Library index rows (performers, studios, movies): alphabetical by folder name…, Store a confirmed external profile URL/ID for a performer. site: tmdb, iafd,…, api_favourites_apply_image(), api_favourites_clear_ext_link() (+203 more)

### Community 3 - "Community 3"
Cohesion: 0.02
Nodes (5): connectedCallback(), handleSlotChange(), updateScroll(), getTimeUntilNextUnit(), render()

### Community 4 - "Community 4"
Cohesion: 0.02
Nodes (174): activity_log_append(), activity_log_append_batch(), activity_log_clear(), activity_log_count(), activity_log_fetch(), activity_log_iter_all(), activity_log_prune(), app_json_cache_delete() (+166 more)

### Community 5 - "Community 5"
Cohesion: 0.02
Nodes (15): addDecorator(), addEffect(), boxIntersect(), checkReady(), extrema(), hslToRgb(), hue2rgb(), initialize() (+7 more)

### Community 6 - "Community 6"
Cohesion: 0.12
Nodes (6): hasDefaultSlot(), hasNamedSlot(), test(), WaColorPickerSwatch, HTMLElementTagNameMap, HTMLElementTagNameMap

### Community 7 - "Community 7"
Cohesion: 0.02
Nodes (122): api_queue_suggestions(), _best_name_match(), _classify_content(), _collect_filtered_tpdb_movies(), _content_filters_fingerprint(), _extract_movie_genders(), _extract_movie_genders_with_fallback(), _fetch_fansdb_performers_feed_scenes() (+114 more)

### Community 8 - "Community 8"
Cohesion: 0.02
Nodes (5): getSeparator(), handleSlotChange(), render(), handleCopy(), showStatus()

### Community 9 - "Community 9"
Cohesion: 0.03
Nodes (104): Exception, api_downloads_remove(), api_iafd_movie_breakdown(), api_queue_suggestions_iafd_cast(), _apply_iafd_trickle_schedule(), build_nfo(), emit(), _ensure_parsed_filename() (+96 more)

### Community 10 - "Community 10"
Cohesion: 0.04
Nodes (51): addOpenListeners(), constructor(), disconnectedCallback(), firstUpdated(), handleDialogCancel(), handleDialogClick(), handleDialogPointerDown(), handleOpenChange() (+43 more)

### Community 11 - "Community 11"
Cohesion: 0.03
Nodes (83): best_image_url(), api_favourites_image_search(), api_favourites_studio_detail(), api_studios_external_panel(), best_studio_image_url(), build_group_tvshow_nfo(), build_performer_tvshow_nfo(), build_studio_tvshow_nfo() (+75 more)

### Community 12 - "Community 12"
Cohesion: 0.03
Nodes (74): BaseEvents, BaseProps, @builder.io/qwik, CSSProperties, CustomCssProperties, CustomElements, hono, IntrinsicElements (+66 more)

### Community 13 - "Community 13"
Cohesion: 0.04
Nodes (17): animateCollapse(), animateExpand(), firstUpdated(), getChildrenItems(), handleChildrenSlotChange(), handleExpandAnimation(), handleExpandedChange(), handleLoadingChange() (+9 more)

### Community 14 - "Community 14"
Cohesion: 0.03
Nodes (68): handleSlotChange(), hrefChanged(), setRenderType(), CustomElements, GlobalComponents, IntrinsicElements, JSX, vue (+60 more)

### Community 15 - "Community 15"
Cohesion: 0.03
Nodes (64): _applyFolderDbLinks(), _buildIafdPerformerIndex(), _bulkFolderState, closeQueueFilmstrip(), closeQueueFilmstripLightbox(), closeQueueImageOverlay(), _EXTRA_KIND_REGEXES, _filingNowFilenames (+56 more)

### Community 16 - "Community 16"
Cohesion: 0.03
Nodes (77): api_favourites_folder_logo(), api_favourites_match(), api_favourites_unmatch(), api_movies_info(), api_performers_headshots_by_name(), api_scene_info(), api_studios_enrich_bulk(), _background_enrich_all_studios() (+69 more)

### Community 17 - "Community 17"
Cohesion: 0.04
Nodes (23): click(), constructLightDOMButton(), handleClick(), handleHrefChange(), isButton(), isLink(), render(), connectedCallback() (+15 more)

### Community 18 - "Community 18"
Cohesion: 0.06
Nodes (66): applyBioCellFanart(), buildPerfProwlarrTile(), buildProfilePillsHtml(), buildSocialLinksHtml(), clearStageBanner(), closeAddToLibraryModal(), closeGroupMembersModal(), closePerformerLinkSearchModal() (+58 more)

### Community 19 - "Community 19"
Cohesion: 0.04
Nodes (65): Remove the `[ts-XXXXXXXX]` tag from a job/file name. Returns (cleaned_name,…, _strip_ts_uid(), Connection, _backfill_processed_files_path_columns(), _filename_stem_ext(), get_file_scope(), get_media_duration(), get_parsed_meta() (+57 more)

### Community 20 - "Community 20"
Cohesion: 0.05
Nodes (65): Return cached performers whose cached_at is within max_age_days., Persist the top 100 performers. Purges entries older than 30 days. Uses INSERT…, Delete performer rows whose last_seen is older than ttl_days. Returns the…, Return all cached performer rows, optionally filtered to those seen within the…, Return {source: row_count} so the scheduler can detect empty cache / newly-…, spotlight_evict_stale(), spotlight_get_excluded_ids(), spotlight_load_top_cache() (+57 more)

### Community 21 - "Community 21"
Cohesion: 0.05
Nodes (54): closeAboutModal(), closePerfAliasModal(), closePerfSearchModal(), closeTreeLeafLookup(), _DISK_UNIT_LABELS, dismissHealthStatsHint(), _duplicateGroups, _dupReviewQueue (+46 more)

### Community 22 - "Community 22"
Cohesion: 0.06
Nodes (56): addPageClass(), bindHeaderNavLayoutListener(), buildOverlay(), buildSidebarFromHeaderNav(), closeHeaderNavMenu(), closeOverlay(), closeSidebarDrawer(), collectSidebarEntries() (+48 more)

### Community 23 - "Community 23"
Cohesion: 0.03
Nodes (63): BaseEvents, BaseProps, svelteHTML, WaAnimatedImageProps, WaAnimationProps, WaAvatarProps, WaBadgeProps, WaBreadcrumbItemProps (+55 more)

### Community 25 - "Community 25"
Cohesion: 0.05
Nodes (60): Exact slug lookup. Returns the row dict or None., Primary-key lookup. Used by the manager endpoints so per-row delete / refetch…, Every row, ordered by name. Used by the substring fallback path., Rows that have a logo file on disk — the "Logo library" listing. Used by the…, Rows that don't have a logo file yet — fetcher targets these. Ordered by oldest…, Count of rows still eligible for a fetcher pass., Dead-letter list: rows that have failed enough times to be parked. Used by the…, Normalised lookup key for the studio_logos table. Mirrors main.normalise()… (+52 more)

### Community 26 - "Community 26"
Cohesion: 0.04
Nodes (58): set_library_ownership(), _activity_log_prune_job(), api_news_feed(), _apply_library_ownership_settings(), _apply_tpdb_sync_schedule(), _emit_raw(), _maybe_hydrate_news_rss_cache_from_disk(), _migrate_sidecar_phashes() (+50 more)

### Community 27 - "Community 27"
Cohesion: 0.04
Nodes (58): api_settings_stashdb_tag_search(), api_settings_tag_search(), api_settings_tpdb_tag_search(), _fetch_javstash_scene_for_filing(), _fetch_scene_info_stashbox(), _javstash_movie_search_first_scene_for_pipeline(), _javstash_scenes_for_movie_search(), _library_scene_backfill_worker() (+50 more)

### Community 29 - "Community 29"
Cohesion: 0.04
Nodes (57): Lock, api_cache_clear(), _build_search_term(), _discover_cache_warm_worker(), discover_cached_image(), _discover_download_to_cache_path(), _discover_ensure_cached_image_url(), _discover_image_cache_dir() (+49 more)

### Community 30 - "Community 30"
Cohesion: 0.05
Nodes (53): _library_index_queue_matching_enabled(), _performer_weight(), Scoring weight for the scene-filing router's performer pick. Resolution order:…, Prefer saved folder links from the library entity index when filing the queue., _studio_crosswalk_ids(), _apply_library_ownership(), _ensure_library_filed_permissions(), Path (+45 more)

### Community 31 - "Community 31"
Cohesion: 0.05
Nodes (21): connectedCallback(), constructor(), setInitialAttributes(), checkValidity(), connectedCallback(), firstUpdated(), formDisabledCallback(), formResetCallback() (+13 more)

### Community 32 - "Community 32"
Cohesion: 0.09
Nodes (53): applyTvshowNfoFromIndex(), autoMatchAllLibVideos(), autoMatchLibVideo(), closeDupCompareModal(), closeLibVideoMatch(), deleteAllEmptyFolders(), deleteAllNfo(), deleteFolder() (+45 more)

### Community 33 - "Community 33"
Cohesion: 0.05
Nodes (53): normalise(), Lowercase compare key for folder names vs API names (hyphens, dots, underscores…, _apply_gender_allowlist(), apply_rename_map(), _ext_link_preview_thumb(), _ext_link_url_search_hit(), _favourites_name_index(), _fetch_performer_aliases() (+45 more)

### Community 34 - "Community 34"
Cohesion: 0.09
Nodes (40): a(), n(), addPropertyDecorator(), e(), i(), l(), r(), s() (+32 more)

### Community 35 - "Community 35"
Cohesion: 0.04
Nodes (22): HTMLElementTagNameMap, WaBadge, HTMLElementTagNameMap, WaBreadcrumb, HTMLElementTagNameMap, WaCallout, HTMLElementTagNameMap, WaCarouselItem (+14 more)

### Community 36 - "Community 36"
Cohesion: 0.11
Nodes (45): applyDynamicOverflow(), _bannerBusy(), _bannerIsInSidebar(), _bannerMoreChipHtml(), bannerMoreHtml(), _bannerMoreRowFromChip(), _broadcastFilingNow(), circleDownloads() (+37 more)

### Community 37 - "Community 37"
Cohesion: 0.05
Nodes (6): handleKeyDown(), submitForm(), submitOnEnter(), handleKeyDown(), handleStepperPointerUp(), render()

### Community 38 - "Community 38"
Cohesion: 0.05
Nodes (32): applyPickedImage(), clearFavSearch(), closeImagePickModal(), _EXT_LINK_MODAL_SOURCES, _EXT_SEARCH_SITES, _FAV_VIEW_TITLES, _favPanelProwlarrResults, _favSearchMatchLibraryRow() (+24 more)

### Community 39 - "Community 39"
Cohesion: 0.11
Nodes (43): applyCellFanart(), applyPickedLogo(), applyStudioLogoLink(), closeLinkSearchModal(), closeLogoPickModal(), closeStudioAddModal(), closeStudioPopup(), ensureLinkSearchModal() (+35 more)

### Community 40 - "Community 40"
Cohesion: 0.06
Nodes (42): feed_display_pool_get(), feed_display_pool_upsert(), feed_display_pools_clear_kind(), Return ``{"items": [...], "filter_key": str}`` or ``None`` if missing., Replace the stored JSON payload for one pool slot., Wipe every pool row for ``kind`` (``scenes`` or ``movies``)., api_home_data(), _filter_feed_cards() (+34 more)

### Community 41 - "Community 41"
Cohesion: 0.07
Nodes (42): library_has_phash(), library_match_movies(), _normalize_wanted_source(), Upsert a wanted item. Returns the row id. Resets ``acquired_at`` so re-adding…, Return wanted items as dicts, newest first., Lightweight ``{kind, source, external_id}`` tuples for active items — used to…, Mark a single wanted row acquired by primary key — used by the predicted-path…, Save a StashDB or FansDB cross-reference on an existing wanted row. (+34 more)

### Community 42 - "Community 42"
Cohesion: 0.09
Nodes (34): applyDlSearchModeUi(), cleanRssTitle(), dbCheckWantedItem(), _dlProwlarrResults, _dlProwlarrResultsFiltered, _dlRssCacheGet(), _dlRssPaintFromCache(), _dlRssResolveCacheHit() (+26 more)

### Community 43 - "Community 43"
Cohesion: 0.05
Nodes (17): _dbSources, _ensureTagActionPopup(), _FEED_MODE_SOURCES, _FEED_MODES, _feedCache, _libCheckCache, _LOCAL_LOGOS, _monitoredTagsCache (+9 more)

### Community 44 - "Community 44"
Cohesion: 0.06
Nodes (24): ALL_PERF_GENDERS, _blacklistTags, CUSTOM_DEFAULTS_DARK, deleteStudioLogoRow(), DIR_GENDER_ICONS, dirs, _dlClientConfigFromForm(), loadStudioLogosUnresolvable() (+16 more)

### Community 45 - "Community 45"
Cohesion: 0.08
Nodes (38): _applyFilingNowClasses(), _applyMovieQueuePayload(), _applySceneQueuePayload(), _bulkFolderClassifyFolder(), clearAllQueueFilters(), _cmpQueueFiles(), _computeRenderSig(), filterActiveQueue() (+30 more)

### Community 46 - "Community 46"
Cohesion: 0.06
Nodes (19): connectedCallback(), isNestedItem(), updateIndentation(), constructor(), focusItem(), getAllTreeItems(), getExpandButtonIcon(), getFocusableItems() (+11 more)

### Community 47 - "Community 47"
Cohesion: 0.07
Nodes (36): app_json_cache_get(), app_json_cache_put(), get_movie_by_filename(), notification_add(), Return parsed JSON payload or ``None``., Stash the computed phash onto an existing wanted row so future phash-only scans…, Ephemeral UI notification (header strip). Returns new id or None., update_movie() (+28 more)

### Community 48 - "Community 48"
Cohesion: 0.10
Nodes (24): handleKeyDown(), clamp(), handleKeyDown(), updated(), clampAndRoundToStep(), firstUpdated(), getPercentageFromValue(), getValueFromCoordinates() (+16 more)

### Community 49 - "Community 49"
Cohesion: 0.10
Nodes (22): addOpenListeners(), getFormattedValue(), getHexString(), handleAlphaKeyDown(), handleEyeDropper(), handleFormatChange(), handleFormatToggle(), handleGridKeyDown() (+14 more)

### Community 50 - "Community 50"
Cohesion: 0.07
Nodes (15): _appendStrictSceneParams(), clearSplitClasses(), done(), _humanDuration(), _libraryRegexCache, _loadInfoPane(), onKey(), _paintInfoPaneData() (+7 more)

### Community 51 - "Community 51"
Cohesion: 0.08
Nodes (13): FileSystemEventHandler, DownloadWatchHandler, JavDownloadWatchHandler, MovieDownloadWatchHandler, NewMovieVideoHandler, NewVideoHandler, Watch movies_source_dir; queue video files after hold (same settings as scene…, Watches the download folder and queues new entries after a hold period. (+5 more)

### Community 52 - "Community 52"
Cohesion: 0.06
Nodes (7): HTMLElementTagNameMap, HTMLElementTagNameMap, WaNumberInput, HTMLElementTagNameMap, WaRadioGroup, HTMLElementTagNameMap, ../../internal/webawesome-form-associated-element.js

### Community 53 - "Community 53"
Cohesion: 0.13
Nodes (26): activeElements(), addToSubmenuStack(), cleanupSubmenuPosition(), closeAllSubmenus(), closeSiblingSubmenus(), constructor(), disconnectedCallback(), firstUpdated() (+18 more)

### Community 54 - "Community 54"
Cohesion: 0.13
Nodes (30): buildMenuItems(), changeDirectory(), closeMenu(), _closeMergeModal(), closeMoveModal(), closeRenameModal(), confirmDeleteDisk(), confirmRemove() (+22 more)

### Community 55 - "Community 55"
Cohesion: 0.07
Nodes (32): _build_library_match_indexes(), enrich_download_items_with_filing(), _entry_id(), favourite_group_add_id(), favourite_group_remove_id(), favourite_lookup_performer_by_alias(), favourite_lookup_performer_by_name(), _group_ids_load() (+24 more)

### Community 56 - "Community 56"
Cohesion: 0.07
Nodes (32): _check_pending_downloads(), _check_pending_files(), _check_pending_movies(), _clean_and_rename_on_disk(), _clean_release_filename(), _dl_path_under_movies_source(), _dl_resolve_import_dest_dir(), _health_enumerate_videos_under_roots() (+24 more)

### Community 58 - "Community 58"
Cohesion: 0.07
Nodes (31): library_file_delete_by_destination(), library_files_active_matching_phash(), library_files_clear_duplicate_pending(), library_files_duplicate_groups(), library_files_list_active_rows(), library_files_mark_duplicate_pending(), library_files_set_duplicate_ignored_for_phash(), processed_file_delete_by_id() (+23 more)

### Community 59 - "Community 59"
Cohesion: 0.14
Nodes (28): _applyCropTransform(), applyPosterRole(), closeCropModal(), confirmCrop(), displayFolderName(), ensurePosterModals(), _ensurePosterRoleHeaderLayout(), esc() (+20 more)

### Community 60 - "Community 60"
Cohesion: 0.09
Nodes (30): _bfs_find_dir_named(), _candidate_client_path_roots(), _download_client_dest_candidates(), _download_client_dir_name_matches(), _download_path_not_found_client_report(), _find_local_download_under_watch_dirs(), _find_matching_child_dir(), _find_overlap_child_dir() (+22 more)

### Community 61 - "Community 61"
Cohesion: 0.09
Nodes (30): _dl_cat_match(), _dl_collect_bandwidth(), _dl_collect_deluge(), _dl_collect_nzbget(), _dl_collect_qbittorrent(), _dl_collect_sabnzbd(), _dl_collect_transmission(), _dl_resolve_nzb_settings() (+22 more)

### Community 62 - "Community 62"
Cohesion: 0.07
Nodes (3): Validator, WebAwesomeFormAssociatedElement, WebAwesomeFormControl

### Community 63 - "Community 63"
Cohesion: 0.11
Nodes (29): clearScenePick(), clearSearch(), _clearSearchFrames(), clearSelectedFile(), closeManual(), generateThumb(), _loadSearchFrames(), _mResetThumb() (+21 more)

### Community 64 - "Community 64"
Cohesion: 0.12
Nodes (29): closeIafdBulkResults(), closeVice(), confirmDeleteQueueFile(), iafdBulkFileSelected(), loadQueue(), loadQueueStats(), loadSuggestions(), _markQueueStatsStale() (+21 more)

### Community 65 - "Community 65"
Cohesion: 0.24
Nodes (14): getIconFolder(), getIconUrl(), getBasePath(), getIconPath(), getKitCode(), setBasePath(), setIconPath(), setKitCode() (+6 more)

### Community 66 - "Community 66"
Cohesion: 0.12
Nodes (6): connectedCallback(), createAnimation(), destroyAnimation(), disconnectedCallback(), handleAnimationChange(), handleSlotChange()

### Community 67 - "Community 67"
Cohesion: 0.11
Nodes (23): connectedCallback(), constructor(), focus(), formResetCallback(), getAllOptions(), handleClearClick(), handleComboboxKeyDown(), handleDefaultSlotChange() (+15 more)

### Community 68 - "Community 68"
Cohesion: 0.17
Nodes (19): canScrollNext(), canScrollPrev(), createClones(), firstUpdated(), getCurrentPage(), getPageCount(), getSlides(), goToSlide() (+11 more)

### Community 69 - "Community 69"
Cohesion: 0.09
Nodes (14): QueryStashboxParsingTests, QueryWithFallbackTests, Smoke tests for the StashDB → TPDB → FansDB GraphQL fallback chain. Targets…, An empty list (not an exception) still continues the chain., Every source returns []; the chain reports 'none' not error., If even JAVStash blows up the function should raise — silent empty returns…, Light coverage of `query_stashbox` response parsing — verifies that both the…, `findScenesByFullFingerprints` returns a flat list. (+6 more)

### Community 71 - "Community 71"
Cohesion: 0.10
Nodes (26): dupModalStatsLine(), embSrcLogo(), esc(), fmtSize(), libSceneViewUrl(), loadDupModalSideData(), _movRenderMatchResult(), movRunSearch() (+18 more)

### Community 72 - "Community 72"
Cohesion: 0.08
Nodes (25): enrich_download_items_with_source_scene(), favourite_delete_missing_paths(), favourite_get_by_path(), favourite_refresh_all_path_existence(), favourite_upsert_folder(), get_history_paged(), library_files_delete_under_folder(), library_files_find_processed_match_for_stem() (+17 more)

### Community 73 - "Community 73"
Cohesion: 0.13
Nodes (25): Element, _atom_link_href(), _infer_release_urls(), _local_xml_tag(), _news_extract_image(), _news_fetch_one(), _news_parse_feed(), _news_strip_html() (+17 more)

### Community 74 - "Community 74"
Cohesion: 0.11
Nodes (19): handleDrag(), handlePositionInPixelsChange(), pixelsToPercentage(), handleDrag(), blur(), constructor(), focus(), handleAlphaDrag() (+11 more)

### Community 75 - "Community 75"
Cohesion: 0.13
Nodes (24): create_session(), get_password_hash(), get_session_hours(), get_session_minutes(), get_session_remaining_seconds(), Back-compat shim — derive from session_minutes (authoritative)., Pure read — return True iff the session exists and has not idle-expired. Does…, Slide the session's idle window forward. Returns remaining seconds after the… (+16 more)

### Community 76 - "Community 76"
Cohesion: 0.13
Nodes (24): fmtCount(), healthRemoveRow(), perfApplyFilter(), step(), _perfApplyStored(), perfClearHeadshot(), perfClearLink(), perfFetchExtLinks() (+16 more)

### Community 77 - "Community 77"
Cohesion: 0.11
Nodes (24): acceptSuggestion(), restoreOnFailure(), _enrichIafdCardsCast(), _fetchHeadshotsByName(), _findSuggestionCard(), _fmtDuration(), _fmtResLabel(), _hydratePerfPiles() (+16 more)

### Community 78 - "Community 78"
Cohesion: 0.12
Nodes (15): addToAriaLabelledBy(), connectedCallback(), disconnectedCallback(), handleForChange(), removeFromAriaLabelledBy(), uniqueId(), constructor(), focus() (+7 more)

### Community 79 - "Community 79"
Cohesion: 0.13
Nodes (10): connectedCallback(), handleChange(), handleInput(), handleRowsChange(), handleValueChange(), scheduleCountAnnouncement(), setRangeText(), setTextareaDimensions() (+2 more)

### Community 80 - "Community 80"
Cohesion: 0.21
Nodes (21): Any, build_sprite(), build_sprite_seeks(), build_sprite_single_ffmpeg(), compute_phash_from_image(), ffprobe_format_and_video0(), file_stat_created_iso(), get_sprite_screenshot() (+13 more)

### Community 81 - "Community 81"
Cohesion: 0.11
Nodes (22): mark_download_import_done(), notifications_delete_expired(), notifications_fetch_active(), Record that a client job was cleared after download watch processing (dedup for…, was_download_import_done(), api_activity_banner(), _banner_downloads_snapshot(), _build_downloads_snapshot() (+14 more)

### Community 82 - "Community 82"
Cohesion: 0.16
Nodes (21): _clearTagSource(), _escHtml(), _mergeTagPick(), _normTagName(), _removeTagFromList(), _removeTagSource(), renderBlacklistTagChips(), _runViceSearch() (+13 more)

### Community 83 - "Community 83"
Cohesion: 0.15
Nodes (19): _candidate_performer_names(), _name_match_tokenize(), _performer_name_tokens_match(), Lowercased non-noise word set for scoring overlap., True when every significant token in ``query_name`` appears in ``cast_name``., Every library-detected performer from the filename must map to cast., Normalized performer list from library guess / split query., Fraction of query tokens present in the candidate's title + studio + performer… (+11 more)

### Community 84 - "Community 84"
Cohesion: 0.17
Nodes (20): applyIndexProgress(), checkIndexProgressOnce(), closeImgUploadModal(), load(), lockAllMatches(), openLocalStashFromBtn(), openLocalStashFromFavouriteRow(), refreshAll() (+12 more)

### Community 85 - "Community 85"
Cohesion: 0.16
Nodes (20): clearAllFavFilters(), cycleFavFilterValue(), _ensureFavLazyObserver(), _favGoToPage(), filtersActive(), normalizeUnlinkFilterValue(), _refreshFavCount(), render() (+12 more)

### Community 87 - "Community 87"
Cohesion: 0.14
Nodes (19): _stashbox_normalize_match_source(), get_history(), library_file_get_by_destination(), library_file_update_destination_row(), processed_file_get_by_id(), Persist fingerprints for a stash-box scene. Each entry is a dict with at least…, scene_fingerprints_upsert(), _api_source_tag() (+11 more)

### Community 88 - "Community 88"
Cohesion: 0.17
Nodes (11): constructor(), getCurrentZoomIndex(), handleLoad(), isZoomInDisabled(), isZoomOutDisabled(), parseZoomLevels(), render(), syncTheme() (+3 more)

### Community 89 - "Community 89"
Cohesion: 0.12
Nodes (15): c(), createQuaternion(), d(), g(), getValueAtCurrentTime(), interpolateValue(), l(), m() (+7 more)

### Community 90 - "Community 90"
Cohesion: 0.13
Nodes (8): initialize$2(), initiateExpression(), applyEase(), ease(), easeIn(), easeOut(), executeExpression(), seedRandom()

### Community 91 - "Community 91"
Cohesion: 0.13
Nodes (18): _fetch_and_cache_indexers(), _get_indexers(), prowlarr_clients_endpoint(), prowlarr_get_clients(), prowlarr_grab(), _prowlarr_headers(), prowlarr_indexers_endpoint(), prowlarr_indexers_refresh() (+10 more)

### Community 92 - "Community 92"
Cohesion: 0.23
Nodes (18): _claimInflight(), clearMatchesOne(), closeExtLinkModal(), closeSearch(), _findRowAcrossLists(), openEntityDetail(), pickSearch(), refreshOne() (+10 more)

### Community 93 - "Community 93"
Cohesion: 0.18
Nodes (7): connectedCallback(), constructor(), disconnectedCallback(), handleChange(), handleDisabledChange(), startObserver(), stopObserver()

### Community 94 - "Community 94"
Cohesion: 0.13
Nodes (14): getDefaultIconFamily(), getIconLibrary(), registerIconLibrary(), setDefaultIconFamily(), unregisterIconLibrary(), unwatchIcon(), watchIcon(), connectedCallback() (+6 more)

### Community 97 - "Community 97"
Cohesion: 0.12
Nodes (18): crossProduct(), floatEqual(), floatZero(), getIntersection(), joinLines(), lerp(), lerpPoint(), linearOffset() (+10 more)

### Community 98 - "Community 98"
Cohesion: 0.13
Nodes (17): api_favourites_entity_scenes(), favourites_recent_scenes(), Normalise one scene object from /performers/…/scenes or /sites/…/scenes., Safe for TPDB URL path segments; keeps hyphens unencoded (UUIDs)., Path segments to try for /performers/{seg}/scenes or /sites/{seg}/scenes (slug…, Recent scenes for a performer or studio; tries slug/id variants and loose JSON…, Fetch recent scenes for a performer from TPDB., Fetch recent scenes for a studio from TPDB. (+9 more)

### Community 99 - "Community 99"
Cohesion: 0.29
Nodes (16): build_downloads(), build_index(), build_queue(), extract_downloads(), extract_feed_body(), extract_queue(), extract_wanted(), inject_page_mode() (+8 more)

### Community 100 - "Community 100"
Cohesion: 0.19
Nodes (6): connectedCallback(), disconnectedCallback(), handleDisabledChange(), handleSlotChange(), startObserver(), stopObserver()

### Community 101 - "Community 101"
Cohesion: 0.18
Nodes (11): connectedCallback(), constructor(), firstUpdated(), hideNavigation(), render(), slotResizeObserver(), toggleNavigation(), toLength() (+3 more)

### Community 104 - "Community 104"
Cohesion: 0.15
Nodes (17): closeIafdBulkPicker(), _detectExtraKind(), _filmLabel(), iafdBulkChangeFilm(), _iafdBulkFetchBreakdown(), _iafdBulkRefreshFileBtn(), iafdBulkRescanAlias(), iafdBulkSelectAllMatched() (+9 more)

### Community 105 - "Community 105"
Cohesion: 0.15
Nodes (17): closeIafdScenePicker(), _mPerfAcceptSuggestion(), _mPerfCurrentToken(), _mPerfHandleInput(), _mPerfHandleKeydown(), _mPerfLoadLibrary(), _mPerfRenderSuggestions(), _mPerfScoreMatch() (+9 more)

### Community 106 - "Community 106"
Cohesion: 0.20
Nodes (15): _bilinear_resize_nfnt(), _bilinear_weights(), _dct_2d_top_left_8x8(), _go_quickselect_median(), _lee_dct_table(), _lee_forward_dct(), Port of nfnt/resize's ``createWeights8`` for the bilinear kernel. Returns…, Apply nfnt's 1-D bilinear filter along ``axis`` of an uint8 image with shape… (+7 more)

### Community 107 - "Community 107"
Cohesion: 0.14
Nodes (16): closeAllFavMenus(), closeEntityPanel(), displayFolderName(), entityPanelLocalStashBlock(), favSceneExternalUrl(), favSceneSourceLabel(), loadImagePickCandidates(), mountEntityPanelMenu() (+8 more)

### Community 108 - "Community 108"
Cohesion: 0.22
Nodes (3): executeScript(), handleSrcChange(), requestInclude()

### Community 109 - "Community 109"
Cohesion: 0.21
Nodes (6): connectedCallback(), detectSize(), handlePositionChange(), handleResize(), handleVerticalChange(), percentageToPixels()

### Community 111 - "Community 111"
Cohesion: 0.17
Nodes (16): _activeThemeDefaultAccent(), clearAccentOverridePicker(), _fillPrioritySelect(), loadCustomThemeFields(), loadSettingsPage(), onUiThemeChange(), _parseTagListSetting(), _rebuildNzbPriorityOptions() (+8 more)

### Community 112 - "Community 112"
Cohesion: 0.20
Nodes (5): findButton(), handleBlur(), handleFocus(), handleMouseOut(), handleMouseOver()

### Community 113 - "Community 113"
Cohesion: 0.16
Nodes (6): constructor(), defaultLabel(), getText(), handleDefaultSlotChange(), updated(), updateDefaultLabel()

### Community 115 - "Community 115"
Cohesion: 0.16
Nodes (15): confirmCleanLibrary(), fitHealthStatCounterValues(), loadLibraryDiskUsage(), loadLibraryStats(), pollIndexStatus(), pollResults(), refreshHealthProgress(), refreshLibraryDiskPanel() (+7 more)

### Community 116 - "Community 116"
Cohesion: 0.13
Nodes (15): histGoPage(), histLoadPage(), histLoadStats(), histPurgeRemoved(), histRenderPagination(), histScheduleFilter(), histSetFilter(), histSortBy() (+7 more)

### Community 117 - "Community 117"
Cohesion: 0.19
Nodes (15): enrichPerformerNames(), esc(), genderBadge(), _libEscRe(), _libraryAndTokenHighlight(), _libraryCombinedRegex(), _libraryEntryForMatch(), _libraryHighlight() (+7 more)

### Community 118 - "Community 118"
Cohesion: 0.21
Nodes (15): $bm_isInstanceOfArray(), $bm_neg(), div(), getPerpendicularVector(), getProjectingAngle(), isNumerable(), length(), mul() (+7 more)

### Community 119 - "Community 119"
Cohesion: 0.18
Nodes (14): _library_destination_under_sql(), _library_exclude_prefixes_sql(), library_files_count_under_destination(), library_files_count_under_path(), library_files_expand_dir(), library_files_expand_dir_by_destination(), _library_scope_sql(), Optional filter: destination must start with scope_prefix (Health tree scope). (+6 more)

### Community 120 - "Community 120"
Cohesion: 0.22
Nodes (14): doExtLinkSearch(), escAttr(), _extSiteApiKey(), favExternalLinks(), mapSearchHitSourceToStashApi(), movieSearchHitExternalHref(), _openExtLinkForBio(), openExtLinkModal() (+6 more)

### Community 121 - "Community 121"
Cohesion: 0.18
Nodes (6): constructor(), getValueFromPointerPosition(), getValueFromXCoordinate(), render(), roundToPrecision(), setRatingValue()

### Community 122 - "Community 122"
Cohesion: 0.36
Nodes (13): afterChange(), bind(), close(), extSiteApiKey(), _flagLibraryInvalidation(), inject(), openDb(), openExt() (+5 more)

### Community 123 - "Community 123"
Cohesion: 0.18
Nodes (14): _applyFeedModeToggleUI(), _applyScenesDocumentLang(), clearSelectedTags(), _ensureMonitoredTagsLoaded(), _monitoredTagKeys(), _positionTagFilterDropdown(), _reconcileTagSelection(), _renderFeedSourceLogos() (+6 more)

### Community 124 - "Community 124"
Cohesion: 0.21
Nodes (5): prefersReducedMotion(), constructor(), handleDrag(), handleDragEnd(), handleDragStart()

### Community 125 - "Community 125"
Cohesion: 0.28
Nodes (10): parseSpaceDelimitedTokens(), connectedCallback(), disconnectedCallback(), handleDisabledChange(), handleOptionsChange(), handleSlotChange(), parseThreshold(), resolveRoot() (+2 more)

### Community 128 - "Community 128"
Cohesion: 0.19
Nodes (13): stuLogoApplyFilter(), _stuLogoBuildAlphaBar(), stuLogoDelete(), _stuLogoFilterInput(), _stuLogoFirstLetter(), stuLogoLoad(), stuLogoPagerGoto(), stuLogoPagerSetSize() (+5 more)

### Community 129 - "Community 129"
Cohesion: 0.23
Nodes (9): _allFiltersMarkup(), _applyTheme(), _ensureDuotoneSvg(), _filterMarkup(), tbl(), _n255(), _readCookie(), _readSavedTheme() (+1 more)

### Community 130 - "Community 130"
Cohesion: 0.27
Nodes (7): ProcessingPipeline, Path, _scene_dict_from_grab_row(), get_async_client(), query_stashbox(), query_with_fallback(), AsyncClient

### Community 131 - "Community 131"
Cohesion: 0.24
Nodes (3): firstUpdated(), generate(), render()

### Community 132 - "Community 132"
Cohesion: 0.23
Nodes (5): dedent(), getSourceScript(), handleSlotChange(), renderMarkdown(), updateAll()

### Community 134 - "Community 134"
Cohesion: 0.17
Nodes (3): lit, PropertyDeclaration, WebAwesomeElement

### Community 135 - "Community 135"
Cohesion: 0.20
Nodes (12): _bulkFolderLoadFilmstrip(), _bulkFolderPopulateAsync(), _bulkFolderRefreshSelectToggle(), bulkFolderSelectAll(), _bulkFolderTeardownPolls(), bulkFolderToggleRow(), bulkFolderToggleSelectAll(), _bulkFolderUpdateStatusLine() (+4 more)

### Community 136 - "Community 136"
Cohesion: 0.26
Nodes (12): _feedCacheGet(), _feedCacheInvalidate(), _feedCacheKey(), _feedCachePeekAny(), _feedCachePut(), loadFeed(), _maybePrefetchInactiveFeeds(), prefetchSceneMode() (+4 more)

### Community 137 - "Community 137"
Cohesion: 0.17
Nodes (12): createNS(), HShapeElement(), ShapeGroupData(), SVGDropShadowEffect(), SVGFillFilter(), SVGGaussianBlurEffect(), SVGMatte3Effect(), SVGProLevelsFilter() (+4 more)

### Community 138 - "Community 138"
Cohesion: 0.22
Nodes (11): processed_files_delete_by_ids(), processed_files_filed_rows(), processed_files_repair_destination_row_if_found(), If the video exists (exact path or alternate extension), refresh destination*…, Delete processed_files rows by primary key; returns number of rows removed., Filing history rows that reference a library destination (for orphan detection)., health_orphaned(), health_orphaned_clear() (+3 more)

### Community 139 - "Community 139"
Cohesion: 0.22
Nodes (4): Background-refreshed snapshot for hot read endpoints. GET handlers wrapped by…, Latest snapshot. Only the very first call before the worker produces a snapshot…, Schedule a rebuild AND drop the current snapshot. Bumps ``_invalidation_gen``…, _WarmCache

### Community 140 - "Community 140"
Cohesion: 0.33
Nodes (10): build_match_index(), classify_archive(), human(), main(), normalise(), Path, Return {archive_path: (wanted_display_name, match_strategy)} for every archive…, Walk library roots, extract <studio> tag values from NFOs. (+2 more)

### Community 141 - "Community 141"
Cohesion: 0.35
Nodes (5): constructor(), handleDisabledChange(), hasTrigger(), hide(), show()

### Community 142 - "Community 142"
Cohesion: 0.18
Nodes (3): HTMLElementTagNameMap, IconAnimation, WaIcon

### Community 148 - "Community 148"
Cohesion: 0.29
Nodes (11): _cssRgbTriple(), destroyDiskCharts(), diskBuildIncludeQuery(), diskPieColors(), diskSaveIncludeKeys(), kindLabelDisk(), refreshLibraryDiskPanelData(), renderDiskCharts() (+3 more)

### Community 149 - "Community 149"
Cohesion: 0.27
Nodes (11): capDisplayName(), ensureCardHeadshots(), movieDetailSkeleton(), movieTitleDisplay(), movieTitleDisplayHtml(), _movieTitleNatural(), openSceneOverlay(), _preloadMovieDetailChromeOnIntent() (+3 more)

### Community 150 - "Community 150"
Cohesion: 0.20
Nodes (11): _activeSettingsCategoryLabel(), addRssFeedRow(), getRssFeedUrlsForSave(), getRssFeedUrlsFromDomRaw(), removeRssFeedRow(), renderRssFeedList(), renderRssFeedListFromSettings(), revealInvalidSettingsFieldCategory() (+3 more)

### Community 151 - "Community 151"
Cohesion: 0.24
Nodes (11): closePopup(), _embedCtx(), ensureMarkup(), _onEmbedGrabClick(), onGrabClick(), _onPopupKeydown(), _popupCtx(), _prowlarrBarPatternByLabel() (+3 more)

### Community 152 - "Community 152"
Cohesion: 0.20
Nodes (10): _build_file_meta(), _clean_filename_for_search(), _extract_scene_number_from_filename(), _extract_trailing_index_from_filename(), Split a cleaned filename into (full_query, title_only, performer, studio,…, One-time per-file metadata bundle threaded into ``_score_name_match``:…, Pull a scene number out of a filename ('Movie Scene 5.mp4' → 5)., Pull a trailing standalone integer from a filename (1–99 only). Skips when an… (+2 more)

### Community 153 - "Community 153"
Cohesion: 0.31
Nodes (10): _consumeLibraryInvalidation(), _focusBoot(), _kindListFor(), loadActiveView(), _loadKind(), populateRootFilter(), _refreshLibraryKind(), runFavSearch() (+2 more)

### Community 154 - "Community 154"
Cohesion: 0.29
Nodes (9): connectedCallback(), constructor(), disconnectedCallback(), handleAnchorChange(), isVirtualElement(), reposition(), start(), stop() (+1 more)

### Community 159 - "Community 159"
Cohesion: 0.31
Nodes (10): _cardWantedButtonHtml(), decorateLibraryMatches(), dedupeScenes(), _renderFeedCacheHit(), renderJavMovieGrid(), renderMovieGrid(), renderSceneGrid(), runScenesSearch() (+2 more)

### Community 160 - "Community 160"
Cohesion: 0.20
Nodes (10): createSizedArray(), CVCompElement(), CVMaskElement(), DashProperty(), HCompElement(), MaskElement(), ShapeCollection(), ShapePath() (+2 more)

### Community 161 - "Community 161"
Cohesion: 0.31
Nodes (9): build_sprite(), compute_phash(), get_sprite_screenshot(), get_video_duration(), Image, Path, 25 ffmpeg invocations with -ss before -i (input seeking) + Pillow montage, run…, Return the runtime of *video_path* in seconds. Tries three strategies in order:… (+1 more)

### Community 162 - "Community 162"
Cohesion: 0.28
Nodes (4): constructor(), hostDisconnected(), start(), stop()

### Community 163 - "Community 163"
Cohesion: 0.31
Nodes (8): waitForEvent(), firstUpdated(), handleSummaryClick(), handleSummaryKeyDown(), hide(), show(), hide(), show()

### Community 165 - "Community 165"
Cohesion: 0.22
Nodes (3): HTMLElementTagNameMap, VirtualElement, WaPopup

### Community 167 - "Community 167"
Cohesion: 0.42
Nodes (7): ensurePerformerPopup(), ensurePopupBundle(), ensurePosterRolePicker(), ensureStudioPopup(), _idlePreload(), loadAllParallel(), loadScript()

### Community 168 - "Community 168"
Cohesion: 0.39
Nodes (9): addDir(), dirGenderRowHtml(), moveDirDown(), moveDirUp(), _performerDirsSortedForDisplay(), removeDir(), renderDirList(), rerank() (+1 more)

### Community 169 - "Community 169"
Cohesion: 0.31
Nodes (9): _isQueuedTitle(), _normReleaseTitle(), _prowlarrBuildQualFilterChips(), _prowlarrHtmlFilterBar(), _prowlarrPaintResultTable(), _prowlarrRowMatchesFilter(), _refreshQueuedTitles(), _repaintQueuedBadgesIn() (+1 more)

### Community 170 - "Community 170"
Cohesion: 0.25
Nodes (8): library_match_scenes(), Return the subset of `scene_ids` for which we already have at least one…, For each item, return a match descriptor when the scene is in the library, or…, scene_fingerprints_known_ids(), api_library_scenes_in(), api_library_seed_fingerprints(), One-shot: for every successfully-imported file in ``processed_files`` that has…, Bulk "do I have this scene in my library?" lookup. Body accepts either of:…

### Community 171 - "Community 171"
Cohesion: 0.29
Nodes (8): _prowlarr_normalize_title(), _prowlarr_phrase_in_title(), _prowlarr_release_matches_queries(), _prowlarr_strict_scene_matches(), Fold camelCase / PascalCase into separated words, then non-alnum → spaces,…, True if any of ``queries`` has ALL its meaningful tokens present as whole words…, True when ``needle`` appears as a contiguous, word-bounded phrase inside a pre-…, Strict scene-context release filter. Keeps a release only when: (scene title…

### Community 172 - "Community 172"
Cohesion: 0.25
Nodes (8): Best-effort cm parse from TPDB's freeform height string., Split TPDB's `34D-26-36` style measurements into (band, cup, waist, hip)., Normalise TPDB's tattoos/piercings (string or list) to a non-empty list when…, Build a v2 cache payload from a /performers/{id} detail response., _spotlight_tpdb_payload_from_detail(), _tpdb_parse_height_cm(), _tpdb_parse_measurements(), _tpdb_truthy_list()

### Community 176 - "Community 176"
Cohesion: 0.25
Nodes (3): DraggableElement, DraggableElementOptions, DragOptions

### Community 177 - "Community 177"
Cohesion: 0.25
Nodes (7): contributions, html, description-markup, elements, name, $schema, version

### Community 178 - "Community 178"
Cohesion: 0.32
Nodes (8): _currentLogFilter(), logFilterChanged(), logLibAppendLine(), logLibClassifyLine(), logLibPrune(), logLibReload(), logLibStartSSE(), _renderLogPagination()

### Community 179 - "Community 179"
Cohesion: 0.32
Nodes (8): _firstMatchingVice(), generateViceThumb(), openVice(), toggleViceInline(), _vResetThumb(), _vShowRandBtn(), _vShowRegenBtn(), vThumbPreview()

### Community 180 - "Community 180"
Cohesion: 0.32
Nodes (8): addNewsRssFeedRow(), clearNewsRssFeedTint(), getNewsRssFeedEntriesFromDomRaw(), getNewsRssFeedUrlsForSave(), removeNewsRssFeedRow(), renderNewsRssFeedList(), renderNewsRssFeedListFromSettings(), restoreNewsRssDefaults()

### Community 188 - "Community 188"
Cohesion: 0.52
Nodes (6): closeMoviePopup(), _ensureCss(), _ensureOverlay(), _genderBadge(), openMoviePopup(), _skeleton()

### Community 189 - "Community 189"
Cohesion: 0.48
Nodes (7): clearMovieSearchPick(), closeMovieSearchPanel(), pickMovieJavstash(), pickMovieTmdb(), pickMovieTpdb(), _refreshAfterMoviePick(), _toastIfQueued()

### Community 190 - "Community 190"
Cohesion: 0.38
Nodes (7): fitSkyCounter(), fitSkyCounterNow(), flushPending(), init(), padIfSky(), padSkyCounter(), scheduleFit()

### Community 191 - "Community 191"
Cohesion: 0.33
Nodes (6): get_movie_rows_map(), All processed_movies rows keyed by filename (for queue UI)., Walk processed_movies rows whose source file should still be present (anything…, reconcile_movies_source(), _build_movies_queue_snapshot(), Compute the full /api/movies/queue payload — reconcile orphaned rows + DB rows-…

### Community 192 - "Community 192"
Cohesion: 0.33
Nodes (6): manual_job_list_active(), manual_job_mark_pending(), Flip an interrupted (status=running at startup) job back to pending so the…, Every pending + running row in insertion order — used at startup to rehydrate…, _manual_action_rehydrate(), Walk `manual_queue_jobs` at startup and re-enqueue every pending / interrupted…

### Community 193 - "Community 193"
Cohesion: 0.33
Nodes (3): constructor(), firstUpdated(), updateHasSubmenuState()

### Community 194 - "Community 194"
Cohesion: 0.33
Nodes (3): constructor(), start(), toggle()

### Community 202 - "Community 202"
Cohesion: 0.53
Nodes (6): _perfDisableJobBtns(), _perfEnableJobBtns(), perfPollProgress(), perfStartFillGaps(), perfStartHeadshotsOnly(), perfStartProgressPoll()

### Community 203 - "Community 203"
Cohesion: 0.40
Nodes (6): _tagPillsAdd(), _tagPillsBindInput(), _tagPillsClear(), _tagPillsRemoveAt(), _tagPillsRender(), _tagPillsSet()

### Community 204 - "Community 204"
Cohesion: 0.33
Nodes (6): addVice(), deleteVice(), _loadVicesFromServer(), removeTagFromVice(), renameVice(), renderVicesList()

### Community 205 - "Community 205"
Cohesion: 0.40
Nodes (6): _contrastRatio(), _hexToRgb(), onCustomFieldChange(), readCustomThemeFields(), _relLum(), updateCustomContrastBadge()

### Community 215 - "Community 215"
Cohesion: 0.40
Nodes (4): IconLibrary, IconLibraryHostElement, IconLibraryMutator, IconLibraryResolver

### Community 218 - "Community 218"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaCopyErrorEventDetail, WaCopyEvent

### Community 219 - "Community 219"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaCreateEvent, WaCreateEventDetail

### Community 220 - "Community 220"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaHideEvent, WaHideEventDetails

### Community 221 - "Community 221"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaHoverEvent, WaHoverEventDetail

### Community 222 - "Community 222"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaIncludeErrorDetail, WaIncludeErrorEvent

### Community 223 - "Community 223"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaIntersectEvent, WaIntersectEventDetail

### Community 224 - "Community 224"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaMutationEvent, WaMutationEventDetail

### Community 225 - "Community 225"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaResizeEvent, WaResizeEventDetail

### Community 226 - "Community 226"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaSelectEvent, WaSelectEventDetail

### Community 227 - "Community 227"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaSelectionChangeEvent, WaSelectionChangeEventDetail

### Community 228 - "Community 228"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaSlideChangeEvent, WaSlideChangeEventDetails

### Community 229 - "Community 229"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaTabHideEvent, WaTabHideEventDetail

### Community 230 - "Community 230"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaTabShowEvent, WaTabShowEventDetail

### Community 231 - "Community 231"
Cohesion: 0.40
Nodes (3): GlobalEventHandlersEventMap, WaVideoChangeEvent, WaVideoChangeEventDetail

### Community 233 - "Community 233"
Cohesion: 0.40
Nodes (4): NonUndefined, UpdateHandler, UpdateHandlerFunctionKeys, WatchOptions

### Community 234 - "Community 234"
Cohesion: 0.80
Nodes (5): _attachTagActionDismiss(), _detachTagActionDismiss(), _hideTagActionPopup(), _tagActionDismissClick(), _tagActionDismissKey()

### Community 235 - "Community 235"
Cohesion: 0.70
Nodes (5): _applyDuotoneAttr(), _duotonePageKey(), _getDuotoneInverted(), initDuotoneMode(), toggleDuotoneMode()

### Community 236 - "Community 236"
Cohesion: 0.60
Nodes (5): addBrightnessToRGB(), addHueToRGB(), addSaturationToRGB(), HSVtoRGB(), RGBtoHSV()

### Community 237 - "Community 237"
Cohesion: 0.50
Nodes (4): _perf_rename_collect_video_jobs(), _perf_rename_stem_starts_with(), If ``stem`` begins with ``old_name`` followed by the canonical ` - S####E####`…, Walk ``folder`` for video files that begin with the canonical `<old_name> -…

### Community 238 - "Community 238"
Cohesion: 0.50
Nodes (4): v3: Single gamma-compressed weighted sample over the whole pool, plus an…, Weighted sample without replacement from the spotlight buffer. Weight model…, _spotlight_split_sample(), _spotlight_weighted_sample()

### Community 258 - "Community 258"
Cohesion: 0.50
Nodes (3): clientFixture, hydratedFixture, Window

### Community 259 - "Community 259"
Cohesion: 0.50
Nodes (3): $schema, tags, version

### Community 260 - "Community 260"
Cohesion: 0.67
Nodes (4): clearScenesSearch(), _scenesSrchSelectedSources(), toggleScenesSrchAllSources(), _updateScenesSrchSourceState()

### Community 261 - "Community 261"
Cohesion: 0.67
Nodes (3): _file_stat_created_iso(), stat_result, Best-effort file creation / birth date for library UI (YYYY-MM-DD).

## Knowledge Gaps
- **431 isolated node(s):** `_lastSearchResults`, `_rowsPerf`, `_rowsStudio`, `_rowsMovie`, `_rowsVice` (+426 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **81 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `get_conn()` connect `Community 4` to `Community 0`, `Community 1`, `Community 2`, `Community 9`, `Community 138`, `Community 16`, `Community 19`, `Community 20`, `Community 25`, `Community 26`, `Community 27`, `Community 40`, `Community 41`, `Community 170`, `Community 47`, `Community 55`, `Community 58`, `Community 191`, `Community 192`, `Community 72`, `Community 75`, `Community 81`, `Community 87`, `Community 119`?**
  _High betweenness centrality (0.005) - this node is a cross-community bridge._
- **Why does `get_settings()` connect `Community 1` to `Community 0`, `Community 2`, `Community 4`, `Community 7`, `Community 9`, `Community 11`, `Community 16`, `Community 19`, `Community 20`, `Community 152`, `Community 25`, `Community 26`, `Community 27`, `Community 29`, `Community 33`, `Community 40`, `Community 41`, `Community 47`, `Community 55`, `Community 56`, `Community 61`, `Community 191`, `Community 81`, `Community 87`, `Community 91`, `Community 98`?**
  _High betweenness centrality (0.004) - this node is a cross-community bridge._
- **Why does `health_orphaned_clear()` connect `Community 138` to `Community 0`, `Community 1`?**
  _High betweenness centrality (0.004) - this node is a cross-community bridge._
- **What connects `_lastSearchResults`, `_rowsPerf`, `_rowsStudio` to the rest of the system?**
  _431 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.010186859553948161 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.01436642453591606 - nodes in this community are weakly interconnected._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.01909275558564658 - nodes in this community are weakly interconnected._