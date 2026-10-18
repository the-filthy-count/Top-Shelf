"""Shared country names for comparisons and statistical grouping."""
import unicodedata

_COUNTRY_ALIASES: dict[str, str] = {
    # ── United States ─────────────────────────────────────────────
    # StashDB's `country` field commonly carries the *demonym* ("American")
    # rather than the proper noun, so the demonym must alias to the
    # canonical name or the spotlight filter for "United States" misses
    # the bulk of US performers.
    "us": "united states", "usa": "united states", "u.s.": "united states",
    "u.s.a.": "united states", "united states of america": "united states",
    "american": "united states",
    # ── United Kingdom ────────────────────────────────────────────
    "uk": "united kingdom", "u.k.": "united kingdom", "great britain": "united kingdom",
    "england": "united kingdom", "britain": "united kingdom", "scotland": "united kingdom",
    "wales": "united kingdom", "northern ireland": "united kingdom",
    "british": "united kingdom", "english": "united kingdom", "scottish": "united kingdom",
    "welsh": "united kingdom",
    # ── Canada ────────────────────────────────────────────────────
    "ca": "canada", "can": "canada", "canadian": "canada",
    # ── Australia ─────────────────────────────────────────────────
    "au": "australia", "aus": "australia", "australian": "australia",
    # ── Germany ───────────────────────────────────────────────────
    "de": "germany", "deutschland": "germany", "german": "germany",
    # ── Czech Republic ────────────────────────────────────────────
    "cz": "czech republic", "czech rep.": "czech republic", "czech rep": "czech republic",
    "czechia": "czech republic", "czech": "czech republic",
    # ── France ────────────────────────────────────────────────────
    "fr": "france", "french": "france",
    # ── Spain ─────────────────────────────────────────────────────
    "es": "spain", "spanish": "spain",
    # ── Brazil ────────────────────────────────────────────────────
    "br": "brazil", "brasil": "brazil", "brazilian": "brazil",
    # ── Japan ─────────────────────────────────────────────────────
    "jp": "japan", "japanese": "japan",
    # ── South Korea ───────────────────────────────────────────────
    "kr": "south korea", "korea": "south korea", "korean": "south korea",
    # ── Hungary ───────────────────────────────────────────────────
    "hu": "hungary", "hungarian": "hungary",
    # ── Romania ───────────────────────────────────────────────────
    "ro": "romania", "romanian": "romania",
    # ── Poland ────────────────────────────────────────────────────
    "pl": "poland", "polish": "poland",
    # ── Netherlands ───────────────────────────────────────────────
    "nl": "netherlands", "the netherlands": "netherlands", "holland": "netherlands",
    "dutch": "netherlands",
    # ── Italy ─────────────────────────────────────────────────────
    "it": "italy", "italian": "italy",
    # ── Russia ────────────────────────────────────────────────────
    "ru": "russia", "russian federation": "russia", "russian": "russia",
    # ── Ukraine ───────────────────────────────────────────────────
    "ua": "ukraine", "ukrainian": "ukraine",
    # ── Colombia ──────────────────────────────────────────────────
    "co": "colombia", "colombian": "colombia",
    # ── Argentina ─────────────────────────────────────────────────
    "ar": "argentina", "argentinian": "argentina", "argentine": "argentina",
    # ── Venezuela ─────────────────────────────────────────────────
    "ve": "venezuela", "venezuelan": "venezuela",
    # ── Mexico ────────────────────────────────────────────────────
    "mx": "mexico", "mexican": "mexico",
    # ── Sweden ────────────────────────────────────────────────────
    "se": "sweden", "swedish": "sweden",
    # ── Denmark ───────────────────────────────────────────────────
    "dk": "denmark", "danish": "denmark",
    # ── Norway ────────────────────────────────────────────────────
    "no": "norway", "norwegian": "norway",
    # ── Finland ───────────────────────────────────────────────────
    "fi": "finland", "finnish": "finland",
    # ── Switzerland ───────────────────────────────────────────────
    "ch": "switzerland", "swiss": "switzerland",
    # ── Austria ───────────────────────────────────────────────────
    "at": "austria", "austrian": "austria",
    # ── Belgium ───────────────────────────────────────────────────
    "be": "belgium", "belgian": "belgium",
    # ── Ireland ───────────────────────────────────────────────────
    "ie": "ireland", "irish": "ireland",
    # ── Slovakia ──────────────────────────────────────────────────
    "sk": "slovakia", "slovak": "slovakia",
    # ── New Zealand ───────────────────────────────────────────────
    "nz": "new zealand", "new zealander": "new zealand", "kiwi": "new zealand",
    # ── South Africa ──────────────────────────────────────────────
    "za": "south africa", "south african": "south africa",
    # ── Philippines ───────────────────────────────────────────────
    "ph": "philippines", "filipino": "philippines", "filipina": "philippines",
    # ── Thailand ──────────────────────────────────────────────────
    "th": "thailand", "thai": "thailand",
    # ── Israel ────────────────────────────────────────────────────
    "il": "israel", "israeli": "israel",
    # ── India ─────────────────────────────────────────────────────
    "in": "india", "indian": "india",
    # ── China ─────────────────────────────────────────────────────
    "cn": "china", "prc": "china", "people's republic of china": "china",
    "chinese": "china",
    # ── Taiwan ────────────────────────────────────────────────────
    "tw": "taiwan", "republic of china": "taiwan", "taiwanese": "taiwan",
    # ── Hong Kong ─────────────────────────────────────────────────
    "hk": "hong kong",
    # ── Bulgaria ──────────────────────────────────────────────────
    "bg": "bulgaria", "bulgarian": "bulgaria",
    # ── Serbia ────────────────────────────────────────────────────
    "rs": "serbia", "serbian": "serbia",
    # ── Croatia ───────────────────────────────────────────────────
    "hr": "croatia", "croatian": "croatia",
    # ── Slovenia ──────────────────────────────────────────────────
    "si": "slovenia", "slovenian": "slovenia",
    # ── Bosnia ────────────────────────────────────────────────────
    "ba": "bosnia and herzegovina", "bosnia": "bosnia and herzegovina",
    "bosnian": "bosnia and herzegovina",
    # ── Albania ───────────────────────────────────────────────────
    "al": "albania", "albanian": "albania",
    # ── Moldova ───────────────────────────────────────────────────
    "md": "moldova", "moldovan": "moldova",
    # ── Belarus ───────────────────────────────────────────────────
    "by": "belarus", "belarusian": "belarus",
    # ── Lithuania ─────────────────────────────────────────────────
    "lt": "lithuania", "lithuanian": "lithuania",
    # ── Latvia ────────────────────────────────────────────────────
    "lv": "latvia", "latvian": "latvia",
    # ── Estonia ───────────────────────────────────────────────────
    "ee": "estonia", "estonian": "estonia",
    # ── Kazakhstan ────────────────────────────────────────────────
    "kz": "kazakhstan", "kazakh": "kazakhstan",
    # ── Turkey ────────────────────────────────────────────────────
    "tr": "turkey", "türkiye": "turkey", "turkish": "turkey",
    # ── Greece ────────────────────────────────────────────────────
    "gr": "greece", "greek": "greece",
    # ── Portugal ──────────────────────────────────────────────────
    "pt": "portugal", "portuguese": "portugal",
    # ── Iceland ───────────────────────────────────────────────────
    "is": "iceland", "icelandic": "iceland",
    # ── Chile ─────────────────────────────────────────────────────
    "cl": "chile", "chilean": "chile",
    # ── Peru ──────────────────────────────────────────────────────
    "pe": "peru", "peruvian": "peru",
    # ── Ecuador ───────────────────────────────────────────────────
    "ec": "ecuador", "ecuadorian": "ecuador",
    # ── Bolivia ───────────────────────────────────────────────────
    "bo": "bolivia", "bolivian": "bolivia",
    # ── Paraguay ──────────────────────────────────────────────────
    "py": "paraguay", "paraguayan": "paraguay",
    # ── Uruguay ───────────────────────────────────────────────────
    "uy": "uruguay", "uruguayan": "uruguay",
    # ── Dominican Republic ────────────────────────────────────────
    "do": "dominican republic", "dominican": "dominican republic",
    # ── Cuba ──────────────────────────────────────────────────────
    "cu": "cuba", "cuban": "cuba",
    # ── Jamaica ───────────────────────────────────────────────────
    "jm": "jamaica", "jamaican": "jamaica",
    # ── Puerto Rico ───────────────────────────────────────────────
    "pr": "puerto rico", "puerto rican": "puerto rico",
    # ── Costa Rica ────────────────────────────────────────────────
    "cr": "costa rica", "costa rican": "costa rica",
    # ── Malaysia ──────────────────────────────────────────────────
    "my": "malaysia", "malaysian": "malaysia",
    # ── Indonesia ─────────────────────────────────────────────────
    "id": "indonesia", "indonesian": "indonesia",
    # ── Singapore ─────────────────────────────────────────────────
    "sg": "singapore", "singaporean": "singapore",
    # ── Vietnam ───────────────────────────────────────────────────
    "vn": "vietnam", "viet nam": "vietnam", "vietnamese": "vietnam",
}


_COUNTRY_ALIASES.update({
    'gb': 'united kingdom', 'gbr': 'united kingdom',
    'rus': 'russia', 'jpn': 'japan', 'deu': 'germany',
    'fra': 'france', 'esp': 'spain', 'ita': 'italy',
    'bra': 'brazil', 'cze': 'czech republic', 'ukr': 'ukraine',
})

def normalise_country(raw: str) -> str:
    """Canonicalize known codes/names/demonyms; retain unfamiliar values."""
    value = ' '.join(unicodedata.normalize('NFKC', str(raw or '')).split()).casefold()
    return _COUNTRY_ALIASES.get(value, value)
