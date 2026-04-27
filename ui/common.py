import json
from typing import Any

import requests
import streamlit as st


def api_post(endpoint: str, base_url: str, payload: dict | None = None, files: dict | None = None) -> Any:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        if files:
            response = requests.post(url, data=payload or {}, files=files, timeout=120)
        else:
            response = requests.post(url, json=payload or {}, timeout=120)
    except Exception as error:
        return {"error": str(error)}

    try:
        return response.json()
    except ValueError:
        return {
            "status_code": response.status_code,
            "text": response.text,
        }


def api_get(endpoint: str, base_url: str, params: dict | None = None) -> Any:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = requests.get(url, params=params or {}, timeout=10)
    except Exception as error:
        return {"error": str(error)}

    try:
        return response.json()
    except ValueError:
        return {
            "status_code": response.status_code,
            "text": response.text,
        }


def health_check(base_url: str) -> dict[str, Any]:
    base_url = base_url.strip()
    if not base_url:
        return {"alive": False, "error": "API base URL is empty."}

    endpoints = ["health", "ping", ""]
    last_error = None
    for endpoint in endpoints:
        target_url = f"{base_url.rstrip('/')}/{endpoint}" if endpoint else base_url.rstrip("/")
        try:
            response = requests.get(target_url, timeout=10)
            result = {
                "alive": True,
                "endpoint": target_url,
                "status_code": response.status_code,
            }
            try:
                result["data"] = response.json()
            except ValueError:
                result["text"] = response.text
            return result
        except Exception as error:
            last_error = error
            continue

    return {"alive": False, "error": str(last_error) if last_error else "Unable to reach the API."}


def parse_optional(value: str) -> str | None:
    value = value.strip()
    return value if value else None


def parse_optional_float(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_optional_int(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_comma_list(value: str) -> list[str] | None:
    if not value.strip():
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_group_definitions(value: str) -> list[list[str]]:
    groups: list[list[str]] = []
    for line in value.splitlines():
        line = line.strip()
        if not line:
            continue
        group = [item.strip() for item in line.split(",") if item.strip()]
        if group:
            groups.append(group)
    return groups


def format_response(response: Any) -> None:
    if isinstance(response, dict) and response.get("error"):
        st.error(response["error"])
        return
    if isinstance(response, dict) and response.get("status_code") and response["status_code"] >= 400:
        st.error(f"HTTP {response['status_code']}")
        st.code(response.get("text", ""))
        return
    st.success("Request completed")
    st.json(response)
