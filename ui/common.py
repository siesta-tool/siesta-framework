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


def api_post_binary(endpoint: str, base_url: str, payload: dict | None = None) -> bytes | None:
    """Like api_post, but returns raw response bytes instead of parsing as JSON/text.

    Use for binary downloads (e.g. PNG images): api_post's fallback decodes the
    response as text on a non-JSON body, which corrupts binary content.
    """
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = requests.post(url, json=payload or {}, timeout=120)
        response.raise_for_status()
        return response.content
    except Exception as error:
        st.error(f"Request failed: {error}")
        return None


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


def api_delete(endpoint: str, base_url: str, params: dict | None = None) -> Any:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = requests.delete(url, params=params or {}, timeout=60)
    except Exception as error:
        return {"error": str(error)}

    try:
        return response.json()
    except ValueError:
        return {
            "status_code": response.status_code,
            "text": response.text,
        }


def fetch_namespace_catalog(base_url: str) -> dict[str, list[str]]:
    """Query the manager module for existing storage namespaces and the logs within each."""
    response = api_get("manager/namespaces", base_url)
    catalog: dict[str, list[str]] = {}
    if isinstance(response, dict) and isinstance(response.get("namespaces"), list):
        for namespace in response["namespaces"]:
            name = namespace.get("name")
            if name:
                catalog[name] = sorted(namespace.get("logs") or [])
    return catalog


def namespace_options() -> list[str]:
    """Known storage namespaces from the last-fetched catalog, falling back to a sane default."""
    catalog = st.session_state.get("namespace_catalog") or {}
    return sorted(catalog.keys()) or ["siesta"]


def log_options(storage_namespace: str) -> list[str]:
    """Known log names for a namespace from the last-fetched catalog, falling back to a sane default."""
    catalog = st.session_state.get("namespace_catalog") or {}
    return catalog.get(storage_namespace) or ["example_log"]


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


def _display_response_time(response: dict) -> None:
    if response.get("time") is not None:
        try:
            elapsed = float(response["time"])
            st.metric("Elapsed time", f"{elapsed:.2f}s")
        except (TypeError, ValueError):
            st.caption(f"Elapsed time: {response['time']}")


def format_response(response: Any) -> None:
    if response is None:
        # Nothing to render - the caller (e.g. a failed binary/image request)
        # has already surfaced its own error via st.error.
        return

    if isinstance(response, dict) and response.get("error"):
        st.error(response["error"])
        return

    if isinstance(response, dict) and response.get("status_code") and response["status_code"] >= 400:
        st.error(f"HTTP {response['status_code']}")
        st.code(response.get("text", ""))
        return

    if isinstance(response, dict) and "code" in response:
        code = response.get("code")
        message = response.get("message")
        details = {k: v for k, v in response.items() if k not in {"code", "message"}}

        if code == 200:
            st.success(f"Success ({code})")
            if message:
                st.markdown(f"**{message}**")
        else:
            st.error(f"Response code: {code}")
            if message:
                st.markdown(f"**{message}**")

        _display_response_time(response)
        if details:
            with st.expander("Response details"):
                st.json(details)
        return

    st.info("Response received")
    _display_response_time(response if isinstance(response, dict) else {})
    st.json(response)
