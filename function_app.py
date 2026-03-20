import azure.functions as func
import logging
from main import run_pipeline

app = func.FunctionApp()


@app.timer_trigger(schedule="0 0 5 * * *", arg_name="timer", run_on_startup=False)
def blast_timer_trigger(timer: func.TimerRequest) -> None:
    logging.info("Blast pipeline triggered by timer")
    result = run_pipeline()
    logging.info(f"Pipeline complete: {result}")


@app.route(route="run-pipeline", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def blast_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Blast pipeline triggered by HTTP request")
    try:
        result = run_pipeline()
        return func.HttpResponse(f"Pipeline complete: {result}", status_code=200)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        return func.HttpResponse(f"Pipeline failed: {e}", status_code=500)
