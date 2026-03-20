import azure.functions as func
import logging

app = func.FunctionApp()


@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("OK", status_code=200)


@app.timer_trigger(schedule="0 0 5 * * *", arg_name="timer", run_on_startup=False)
def blast_timer_trigger(timer: func.TimerRequest) -> None:
    from main import run_pipeline
    logging.info("Blast pipeline triggered by timer")
    result = run_pipeline()
    logging.info(f"Pipeline complete: {result}")


@app.route(route="run-pipeline", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def blast_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Blast pipeline triggered by HTTP request")
    try:
        from main import run_pipeline
        result = run_pipeline()
        return func.HttpResponse(f"Pipeline complete: {result}", status_code=200)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        return func.HttpResponse(f"Pipeline failed: {e}", status_code=500)
