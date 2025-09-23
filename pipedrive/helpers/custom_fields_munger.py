from typing import Any, Dict, TypedDict, Optional

import dlt

from ..typing import TDataPage


class TFieldMapping(TypedDict):
    name: str
    normalized_name: str
    options: Optional[Dict[str, str]]
    field_type: str


def update_fields_mapping(
    new_fields_mapping: TDataPage, existing_fields_mapping: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Specific function to perform data munging and push changes to custom fields' mapping stored in dlt's state
    The endpoint must be an entity fields' endpoint
    """
    for data_item in new_fields_mapping:
        # 'edit_flag' field contains a boolean value, which is set to 'True' for custom fields and 'False' otherwise.
        if data_item.get("edit_flag"):
            # Regarding custom fields, 'key' field contains pipedrive's hash string representation of its name
            # We assume that pipedrive's hash strings are meant to be an univoque representation of custom fields' name, so dlt's state shouldn't be updated while those values
            # remain unchanged
            existing_fields_mapping = _update_field(data_item, existing_fields_mapping)
        # Built in enum and set fields are mapped if their options have int ids
        # Enum fields with bool and string key options are left intact
        elif data_item.get("field_type") in {"set", "enum"}:
            options = data_item.get("options", [])
            first_option = options[0]["id"] if len(options) >= 1 else None
            if isinstance(first_option, int) and not isinstance(first_option, bool):
                existing_fields_mapping = _update_field(
                    data_item, existing_fields_mapping
                )
    return existing_fields_mapping


def _update_field(
    data_item: Dict[str, Any],
    existing_fields_mapping: Optional[Dict[str, TFieldMapping]],
) -> Dict[str, TFieldMapping]:
    """Create or update the given field's info the custom fields state
    If the field hash already exists in the state from previous runs the name is not updated.
    New enum options (if any) are appended to the state.
    """
    existing_fields_mapping = existing_fields_mapping or {}
    key = data_item["key"]
    options = data_item.get("options", [])
    new_options_map = {str(o["id"]): o["label"] for o in options}
    existing_field = existing_fields_mapping.get(key)
    if not existing_field:
        existing_fields_mapping[key] = dict(
            name=data_item["name"],
            normalized_name=_normalized_name(data_item["name"]),
            options=new_options_map,
            field_type=data_item["field_type"],
        )
        return existing_fields_mapping
    existing_options = existing_field.get("options", {})
    if not existing_options or existing_options == new_options_map:
        existing_field["options"] = new_options_map
        existing_field["field_type"] = data_item[
            "field_type"
        ]  # Add for backwards compat
        return existing_fields_mapping
    # Add new enum options to the existing options array
    # so that when option is renamed the original label remains valid
    new_option_keys = set(new_options_map) - set(existing_options)
    for key in new_option_keys:
        existing_options[key] = new_options_map[key]
    existing_field["options"] = existing_options
    return existing_fields_mapping


def _normalized_name(name: str) -> str:
    source_schema = dlt.current.source_schema()
    normalized_name = name.strip()  # remove leading and trailing spaces
    return source_schema.naming.normalize_identifier(normalized_name)

def _coerce_to_list(value):
    """
    Normalize the value for 'set' fields into a list of option ids (as strings or numbers).
    Accepts:
      - list/tuple -> returns same (converted to list)
      - int -> [int]
      - str -> if contains commas, split on commas; otherwise treat as single id string
      - None/"" -> []
    """
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, int):
        return [value]
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return []
        # split by comma if present (common case: "1,2,3")
        if "," in v:
            parts = [p.strip() for p in v.split(",") if p.strip() != ""]
            return parts
        # If string looks like a JSON array (e.g. '["1","2"]'), try to parse it
        if v.startswith("[") and v.endswith("]"):
            try:
                import json
                parsed = json.loads(v)
                if isinstance(parsed, (list, tuple)):
                    return list(parsed)
            except Exception:
                pass
        # fallback: single id represented as string
        return [v]
    # fallback: anything iterable -> try converting (but avoid iterating over str because already handled)
    try:
        return list(value)
    except Exception:
        return [value]


def rename_fields(data: TDataPage, fields_mapping: Dict[str, Any]) -> TDataPage:
    if not fields_mapping:
        return data
    for data_item in data:
        for hash_string, field in fields_mapping.items():
            if hash_string not in data_item:
                continue
            field_value = data_item.pop(hash_string)
            field_name = field["name"]
            options_map = field.get("options") or {}

            # MULTI-CHOICE ("set") — coerce to list then map using options_map
            if field_value and field["field_type"] == "set":
                coerced = _coerce_to_list(field_value)
                # map each element using the options_map (keys in state are strings)
                mapped = []
                for enum_id in coerced:
                    # try both str and int keys (options_map keys are strings, but id might be int)
                    mapped_label = options_map.get(str(enum_id))
                    if mapped_label is None and isinstance(enum_id, str) and enum_id.isdigit():
                        # try integer form
                        mapped_label = options_map.get(enum_id)
                    mapped.append(mapped_label if mapped_label is not None else enum_id)
                field_value = mapped

            # SINGLE-CHOICE ("enum") — accept string/int or single-element list
            elif field_value and field["field_type"] == "enum":
                # sometimes enum may come as a list with one item — normalize that
                if isinstance(field_value, (list, tuple)) and len(field_value) == 1:
                    fv = field_value[0]
                else:
                    fv = field_value
                # use mapping if available
                field_value = options_map.get(str(fv), fv)

            # else: other field types left as-is
            data_item[field_name] = field_value
    return data