# Siesta UI

A standalone Streamlit frontend for the Siesta API.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

## Notes

- The UI only calls the existing API endpoints exposed by the Siesta backend.
- It does not add new logic or features beyond the current API surface.
- Set the API base URL in the sidebar to match your running Siesta API instance.
