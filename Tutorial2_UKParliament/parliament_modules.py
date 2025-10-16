# parliament_modules.py
# Wrappers & processors for UK Parliament debates (Data2Neo)

import re
from typing import List, Optional, Tuple
import pandas as pd
import data2neo as d2n
from data2neo.neo4j import Node, Relationship, Subgraph, Attribute, match_nodes

# ---------- types we will be keeping from the excel entries ----------
ALLOWED_TYPES = {"Written questions", "Oral questions", "Proceeding contributions"}

# --- Global caches (shared across wrapper calls) ---
_GLOBAL_PERSON_CACHE: dict[str, Node] = {}
_GLOBAL_PARTY_CACHE: dict[str, Node] = {}
_GLOBAL_MEMBER_OF_CACHE: set[tuple[str, str]] = set()
_GLOBAL_CHAMBER_CACHE: dict[str, Node] = {}
_GLOBAL_SUBJECT_CACHE: dict[str, Node] = {}
_GLOBAL_DEPARTMENT_CACHE: dict[str, Node] = {}

# ---------- name parsing (UK Parliament–aware) ----------
_PREFIXES = [
    "The Rt Hon", "Rt Hon", "The Hon", "Hon",
    "Sir", "Dame", "Lord", "Lady", "Baroness", "Baron",
    "Viscount", "Earl", "Marquess", "Duke",
    "Dr", "Professor", "Prof"
]

_PREFIXES_SORTED = sorted(_PREFIXES, key=len, reverse=True)

_POST_NOMINALS = {
    "MP","MSP","MS","AM",
    "KC","QC","PC","DL","OBE","MBE","CBE","KBE","DBE",
    "FRS","FMedSci","FBA","FRSA"
}

def _strip_post_nominals(s: str) -> str:
    s = s.strip().rstrip(",")
    tokens = s.split()
    while tokens and tokens[-1].rstrip(",") in _POST_NOMINALS:
        tokens.pop()
    return " ".join(tokens).strip()

def _consume_prefixes(s: str) -> Tuple[str, str]:
    s = s.strip()
    consumed = []
    changed = True
    while changed:
        changed = False
        for p in _PREFIXES_SORTED:
            if s.lower().startswith(p.lower() + " "):
                consumed.append(p)
                s = s[len(p):].lstrip()
                changed = True
                break
            if s.lower() == p.lower():
                consumed.append(p)
                s = ""
                changed = True
                break
    return (" ".join(consumed).strip(), s.strip())

def _parse_comma_style(s: str) -> Tuple[str, str, str]:
    left, right = [x.strip() for x in s.split(",", 1)]
    m = re.match(r"^(?:The\s+)?(Baroness|Baron|Lord|Lady|Viscount|Earl|Marquess|Duke)\b", right, re.IGNORECASE)
    if m:
        honor = m.group(1).title()
        return ("", left, honor)
    right = _strip_post_nominals(right)
    parts = right.split()
    first = parts[0] if parts else ""
    last = left if left else (parts[-1] if len(parts) > 1 else "")
    return (first, last, "")

def _parse_name(full_name: Optional[str]) -> Tuple[str, str, str]:
    if not full_name:
        return "", "", ""

    s = str(full_name).strip()
    if not s or s.lower() == "nan":
        return "", "", ""

    s = _strip_post_nominals(s)

    if "," in s:
        return _parse_comma_style(s)

    honorifics, rest = _consume_prefixes(s)

    if " of " in rest and honorifics:
        return ("", rest, honorifics)

    parts = rest.split()
    if not parts:
        return "", "", honorifics
    if len(parts) == 1:
        return "", parts[0], honorifics
    first = parts[0]
    last = parts[-1]
    return first, last, honorifics

# ---------- other helpers ----------
def _split_clean(val: Optional[str]) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    return [v.strip() for v in str(val).split(";") if v and v.strip()]

def _safe(resource, key, default=None):
    try:
        return resource[key]
    except Exception:
        return default

def _has_text(resource, key) -> bool:
    v = _safe(resource, key)
    return v is not None and (not (isinstance(v, float) and pd.isna(v))) and str(v).strip() != ""

def _pair_members_parties(members: List[str], parties: List[str]) -> List[Tuple[str, Optional[str]]]:
    pairs: List[Tuple[str, Optional[str]]] = []

    if len(members) == len(parties) and members:
        return list(zip(members, parties))
    if len(members) == 1 and len(parties) > 1:
        return [(members[0], p) for p in parties]
    if len(members) > 1 and len(parties) == 1:
        return [(m, parties[0]) for m in members]
    
    aff = getattr(d2n.GlobalSharedState, "affiliations", {})

    for m in members:
        known = aff.get(m.strip().lower(), set())
        if known:
            for p in known:
                pairs.append((m, p))
        else:
            pairs.append((m, None))
    return pairs

def _is_missing(val) -> bool:
    import pandas as pd
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    if isinstance(val, str) and (not val.strip() or val.strip().lower() == "nan"):
        return True
    return False

# ---------- processors / wrappers ----------

@d2n.register_subgraph_preprocessor
def FILTER_ALLOWED_TYPES(resource):
    t = _safe(resource, "Type")
    if t is None:
        return None
    if str(t).strip() in ALLOWED_TYPES:
        return resource
    return None

@d2n.register_subgraph_preprocessor
def IF_NOT_EMPTY(resource, attribute_name):
    return resource if _has_text(resource, attribute_name) else None

@d2n.register_attribute_postprocessor
def STRIP_EMPTY_TO_NONE(attribute: Attribute) -> Attribute | None:
    """Return None for empty, blank, or NaN attribute values."""
    if attribute is None:
        return None

    v = attribute.value
    if v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and not v.strip()):
        return None

    # Otherwise, return a new immutable Attribute with the cleaned value
    return Attribute(attribute.key, v)

@d2n.register_subgraph_preprocessor
def PREP_SUBJECTS(resource):
    subj = _safe(resource, "Subject", "")
    resource["Subjects"] = _split_clean(subj)
    return resource

@d2n.register_subgraph_preprocessor
def SKIP_IF_EMPTY(resource, column_name: str):
    """
    Prevents node creation if the given column is missing, NaN, or empty string.
    Works for SubgraphFactories (e.g., NODE, RELATIONSHIP).
    """
    if resource is None:
        return None

    val = resource[column_name] if column_name in resource else None
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    return resource

@d2n.register_subgraph_preprocessor
def SKIP_IF_SUPPLY_MISSING(resource, supply_name: str):
    """
    Prevents creation of a relationship if a supplied node (alias) is missing.
    This stops errors like 'Matcher: The provided resource does not contain the supply ...'.
    """
    if resource is None:
        return None
    if not hasattr(resource, "supplies"):
        return resource
    if supply_name not in resource.supplies:
        return None
    return resource

@d2n.register_wrapper
class STORE_CHAMBER(d2n.SubgraphFactoryWrapper):
    """Ensures Chamber nodes are cached and re-used, and links debates via SUBMITTED_TO."""
    def __init__(self, factory, *args):
        super().__init__(factory)

    def construct(self, resource):
        product = super().construct(resource)
        if resource is None or not product.relationships:
            return Subgraph()

        debate_node = list(product.relationships)[0].start_node
        if debate_node is None:
            return Subgraph()

        uid = _safe(resource, "UID", "")
        if not uid:
            return Subgraph()

        name = _safe(resource, "Legislature", "")
        if not name or not str(name).strip():
            return Subgraph()

        chamber_name = str(name).strip()
        subgraph = Subgraph()

        # Cache or create node
        if chamber_name in _GLOBAL_CHAMBER_CACHE:
            chamber_node = _GLOBAL_CHAMBER_CACHE[chamber_name]
        else:
            with d2n.GlobalSharedState.graph_driver.session() as session:
                matches = match_nodes(session, "Chamber", name=chamber_name)
                if matches:
                    chamber_node = matches[0]
                else:
                    chamber_node = Node("Chamber", name=chamber_name)
                    chamber_node.set_primary_label("Chamber")
                    chamber_node.set_primary_key("name")

            _GLOBAL_CHAMBER_CACHE[chamber_name] = chamber_node

        # Always add the relationship
        rel = Relationship(
            debate_node,
            "SUBMITTED_TO",
            chamber_node,
            key=f"{uid}::SUBMITTED_TO::{chamber_name}",
            date=_safe(resource, "Date")
        )

        subgraph |= chamber_node | rel
        return subgraph

@d2n.register_wrapper
class STORE_SUBJECTS(d2n.SubgraphFactoryWrapper):
    """Ensures Subject nodes are cached, reused, and handles multi-subject entries."""
    def __init__(self, factory, *args):
        super().__init__(factory)

    def construct(self, resource):
        product = super().construct(resource)
        if resource is None or not product.relationships:
            return Subgraph()

        debate_node = list(product.relationships)[0].start_node
        if debate_node is None:
            return Subgraph()

        uid = _safe(resource, "UID", "")
        if not uid:
            return Subgraph()

        subgraph = Subgraph()

        # Extract and split multiple subjects
        subjects_raw = _safe(resource, "Subject", "")
        if _is_missing(subjects_raw):
            return subgraph

        # Split by semicolon and clean
        subjects = [s.strip() for s in str(subjects_raw).split(";") if s.strip()]
        if not subjects:
            return subgraph

        for subject_name in subjects:
            # Check cache
            if subject_name in _GLOBAL_SUBJECT_CACHE:
                subject_node = _GLOBAL_SUBJECT_CACHE[subject_name]
            else:
                # DB lookup fallback
                with d2n.GlobalSharedState.graph_driver.session() as session:
                    matches = match_nodes(session, "Subject", name=subject_name)
                    if matches:
                        subject_node = matches[0]
                    else:
                        subject_node = Node("Subject", name=subject_name)
                        subject_node.set_primary_label("Subject")
                        subject_node.set_primary_key("name")

                # Cache
                _GLOBAL_SUBJECT_CACHE[subject_name] = subject_node

            # Add relationship
            subgraph |= subject_node
            subgraph |= Relationship(
                debate_node,
                "HAS",
                subject_node,
                key=f"{uid}::HAS::{subject_name}"
            )

        return subgraph

@d2n.register_wrapper
class STORE_DEPARTMENT(d2n.SubgraphFactoryWrapper):
    """Creates or reuses Department (CorporateAuthor) nodes and links them (multi-department aware)."""
    def __init__(self, factory, *args):
        super().__init__(factory)

    def construct(self, resource):
        product = super().construct(resource)
        if resource is None or not product.relationships:
            return Subgraph()

        debate_node = list(product.relationships)[0].start_node
        if debate_node is None:
            return Subgraph()

        uid = _safe(resource, "UID", "")
        if not uid:
            return Subgraph()

        subgraph = Subgraph()

        name_raw = _safe(resource, "CorporateAuthor", "")
        if _is_missing(name_raw):
            return subgraph

        # Split multiple departments
        departments = [d.strip() for d in str(name_raw).split(";") if d.strip()]
        if not departments:
            return subgraph

        for dep_name in departments:
            # Cache check
            if dep_name in _GLOBAL_DEPARTMENT_CACHE:
                dep_node = _GLOBAL_DEPARTMENT_CACHE[dep_name]
            else:
                # DB lookup fallback
                with d2n.GlobalSharedState.graph_driver.session() as session:
                    matches = match_nodes(session, "Department", name=dep_name)
                    if matches:
                        dep_node = matches[0]
                    else:
                        dep_node = Node("Department", name=dep_name)
                        dep_node.set_primary_label("Department")
                        dep_node.set_primary_key("name")

                _GLOBAL_DEPARTMENT_CACHE[dep_name] = dep_node

            # Add node and relationship
            subgraph |= dep_node
            subgraph |= Relationship(
                debate_node,
                "ASSIGNED_TO",
                dep_node,
                key=f"{uid}::ASSIGNED_TO::{dep_name}"
            )

        return subgraph

def _text_key(uid: str, subtype: str) -> str:
    return f"{uid}_{subtype}"

@d2n.register_wrapper
class BUILD_TEXTS_AND_LINKS(d2n.SubgraphFactoryWrapper):
    """Builds Text nodes and links them to debates and persons, with caching for Person and Party nodes."""
    def __init__(self, factory):
        super().__init__(factory)
    
    # ---------- Cached access ----------
    def get_person(self, name: str) -> Node:
        if not name:
            return None
        if name not in _GLOBAL_PERSON_CACHE:
            _GLOBAL_PERSON_CACHE[name] = self.ensure_person_node(name)
        return _GLOBAL_PERSON_CACHE[name]

    def get_party(self, name: str) -> Node:
        if not name:
            return None
        if name not in _GLOBAL_PARTY_CACHE:
            _GLOBAL_PARTY_CACHE[name] = self.ensure_party_node(name)
        return _GLOBAL_PARTY_CACHE[name]

    # ---------- Helper: Person ----------
    def ensure_person_node(self, full_name: str) -> Node:
        if not full_name:
            return None
        with d2n.GlobalSharedState.graph_driver.session() as session:
            matches = match_nodes(session, "Person", full_name=full_name)
            if matches:
                return matches[0]
            
        # Create new if not found
        first, last, honor = _parse_name(full_name)
        person = Node(
            "Person",
            full_name=full_name,
            parsed_first=first,
            parsed_last=last,
            honorifics=honor,
        )
        person.set_primary_label("Person")
        person.set_primary_key("full_name")
        return person

    # ---------- Helper: Party ----------
    def ensure_party_node(self, party_name: str) -> Node:
        if not party_name:
            return None
        with d2n.GlobalSharedState.graph_driver.session() as session:
            matches = match_nodes(session, "Party", name=party_name)
            if matches:
                return matches[0]
            
        # Create new if not found
        party = Node("Party", name=party_name)
        party.set_primary_label("Party")
        party.set_primary_key("name")
        return party

    # ---------- Helper: build a Text node ----------
    def build_text_node(self, debate: Node, uid: str, subtype: str, text_value) -> Optional[Node]:
        if text_value is None or (isinstance(text_value, float) and pd.isna(text_value)):
            return None
        text_value = str(text_value).strip()
        if not text_value or text_value.lower() == "nan":
            return None

        text_key = _text_key(uid, subtype)
        node = Node("Text", subtype, TextKey=text_key, TextSubtype=subtype, TextContent=text_value)
        node.set_primary_label("Text")
        node.set_primary_key("TextKey")

        # Link Text to Debate
        return node, Relationship(debate, "CONTAINS", node, key=text_key)

    # ---------- Helper: skip "Speaker" entries ----------
    def _skip_speaker(self, name: str) -> bool:
        """Return True if entry contains 'Speaker' (e.g., 'Evans, Nigel; Speaker')."""
        if not name:
            return False
        return "speaker" in str(name).lower()

    def construct(self, resource):
        product = super().construct(resource)
        if resource is None or not product.relationships:
            return Subgraph()

        debate_node = list(product.relationships)[0].start_node
        if debate_node is None:
            return Subgraph()

        uid = _safe(resource, "UID", "")
        if not uid:
            return Subgraph()

        subgraph = Subgraph()

        # ---------- Build Text nodes first ----------
        texts: dict[str, Node] = {}
        triples = [
            ("DebateText", _safe(resource, "Debate Raw Text")),
            ("QuestionText", _safe(resource, "Written Question Raw Text")),
            ("AnswerText", _safe(resource, "Written Answer Raw Text")),
        ]
        for subtype, textval in triples:
            result = self.build_text_node(debate_node, uid, subtype, textval)
            if result:
                node, rel = result
                texts[subtype] = node
                subgraph |= node | rel

        # ---------- Member relationships ----------
        has_debate_text = "DebateText" in texts
        has_answer_text = "AnswerText" in texts
        has_question_text = "QuestionText" in texts

        lead_names = set(_split_clean(_safe(resource, "Lead Member", "")))

        # --- Member → AUTHORS + HOLDS ---
        members = _split_clean(_safe(resource, "Member", ""))
        member_parties = _split_clean(_safe(resource, "Member Party", ""))

        # Precompute all pairings once
        all_pairs = _pair_members_parties(members, member_parties)
        
        for person_name in members:
            if not person_name or person_name in lead_names:
                continue
            if self._skip_speaker(person_name):
                continue  # skip Speaker entries

            person = self.get_person(person_name)
            subgraph |= person
            subgraph |= Relationship(person, "AUTHORS", debate_node, key=f"{uid}::AUTHORS::{person_name}")

            if has_debate_text:
                dt_node = texts["DebateText"]
                subgraph |= Relationship(person, "HOLDS", dt_node, key=f"{dt_node['TextKey']}::HOLDS::{person_name}")

            # Only keep pairs matching this person
            pairs_for_person = [
                (m, p) for (m, p) in all_pairs
                if m.strip().lower() == person_name.strip().lower()
            ]

            # Add MEMBER_OF for each matching party
            for _, party_name in pairs_for_person:
                if not party_name:
                    continue
                party_name_clean = party_name.strip()
                key = (person_name.strip().casefold(), party_name_clean.casefold())
                if key in _GLOBAL_MEMBER_OF_CACHE:
                    continue  # already linked, skip
                _GLOBAL_MEMBER_OF_CACHE.add(key)

                party = self.get_party(party_name_clean)
                subgraph |= party | Relationship(
                    person, "MEMBER_OF", party, key=f"{person_name}::MEMBER_OF::{party_name_clean}"
                )

        # --- Lead Member → SPONSORS + HOLDS ---
        leads = _split_clean(_safe(resource, "Lead Member", ""))
        lead_parties = _split_clean(_safe(resource, "Lead Member Party", ""))

        # Precompute all pairings once
        all_pairs = _pair_members_parties(leads, lead_parties)

        for person_name in leads:
            if not person_name:
                continue
            if self._skip_speaker(person_name):
                continue  # skip Speaker entries

            person = self.get_person(person_name)
            subgraph |= person
            subgraph |= Relationship(person, "SPONSORS", debate_node, key=f"{uid}::SPONSORS::{person_name}")

            if has_debate_text:
                dt_node = texts["DebateText"]
                subgraph |= Relationship(person, "HOLDS", dt_node, key=f"{dt_node['TextKey']}::HOLDS::{person_name}")
                
            # Only keep pairs matching this person
            pairs_for_person = [
                (m, p) for (m, p) in all_pairs
                if m.strip().lower() == person_name.strip().lower()
            ]

            # Add MEMBER_OF for each matching party
            for _, party_name in pairs_for_person:
                if not party_name:
                    continue
                party_name_clean = party_name.strip()
                key = (person_name.strip().casefold(), party_name_clean.casefold())
                if key in _GLOBAL_MEMBER_OF_CACHE:
                    continue  # already linked, skip
                _GLOBAL_MEMBER_OF_CACHE.add(key)

                party = self.get_party(party_name_clean)
                subgraph |= party | Relationship(
                    person, "MEMBER_OF", party, key=f"{person_name}::MEMBER_OF::{party_name_clean}"
                )

        # --- Answering Member → GIVES ---
        ams = _split_clean(_safe(resource, "Answering Member", ""))
        am_parties = _split_clean(_safe(resource, "Answering Member Party", ""))

        # Precompute all pairings once
        all_pairs = _pair_members_parties(ams, am_parties)

        for person_name in ams:
            if not person_name:
                continue
            if self._skip_speaker(person_name):
                continue  # skip Speaker entries

            person = self.get_person(person_name)
            subgraph |= person
            if has_answer_text:
                at_node = texts["AnswerText"]
                subgraph |= Relationship(person, "GIVES", at_node, key=at_node["TextKey"])
            
            # Only keep pairs matching this person
            pairs_for_person = [
                (m, p) for (m, p) in all_pairs
                if m.strip().lower() == person_name.strip().lower()
            ]

            # Add MEMBER_OF for each matching party
            for _, party_name in pairs_for_person:
                if not party_name:
                    continue
                party_name_clean = party_name.strip()
                key = (person_name.strip().casefold(), party_name_clean.casefold())
                if key in _GLOBAL_MEMBER_OF_CACHE:
                    continue  # already linked, skip
                _GLOBAL_MEMBER_OF_CACHE.add(key)

                party = self.get_party(party_name_clean)
                subgraph |= party | Relationship(
                    person, "MEMBER_OF", party, key=f"{person_name}::MEMBER_OF::{party_name_clean}"
                )

        # --- ANSWERS (link text → text) ---
        if has_answer_text and has_question_text:
            at_node = texts["AnswerText"]
            qt_node = texts["QuestionText"]
            subgraph |= Relationship(at_node, "ANSWERS", qt_node)

        return subgraph