from siesta_framework.core.app import Siesta
from fastapi import FastAPI, APIRouter
import uvicorn

"""
API Router for Siesta Framework.
"""


def startup(siestaInstance: Siesta) -> None:
    """Startup hook to register routes from discovered modules."""
    config = siestaInstance.config
    app = FastAPI(title=config.get("app_name", "Siesta Framework API"))
    router = APIRouter()
    routes = siestaInstance.get_registered_routes()

    # Starting FastAPI app
    for module, registered_routes in routes.items():
        if not registered_routes:
            continue
        for endpoint, route_def in registered_routes.items():
            options = {}
            if len(route_def) == 3:
                method, func, options = route_def
            else:
                method, func = route_def

            route_path = f"/{module}/{endpoint}"

            match method:
                case "GET":
                    router.get(route_path, **options)(func)
                case "POST":
                    router.post(route_path, **options)(func)
                case "PUT":
                    router.put(route_path, **options)(func)
                case "DELETE":
                    router.delete(route_path, **options)(func)
                case _:
                    raise ValueError(f"Unsupported API method: {method}")
                
    app.include_router(router)
    run_config = config.get("api", {"host": "localhost", "port": 8000})
    uvicorn.run(app, host=run_config["host"], port=run_config["port"])