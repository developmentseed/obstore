# obstore FastAPI example

Example returning a streaming response via FastAPI.

```
uv run fastapi dev main.py
```

Note that here FastAPI wraps `starlette.responses`. So any web server that uses
Starlette for responses can use this same code.
