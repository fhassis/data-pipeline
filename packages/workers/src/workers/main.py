
import structlog

from shared.logging import configure_logging


def main():
    """
    Main entry point for the worker process.
    """
    # create the application logger
    logger = structlog.get_logger("my-worker")

    # using snake case in structured logging
    logger.info("worker_executed", user_id=123, status="success")


if __name__ == "__main__":

    # configure application logging
    configure_logging()
    
    # execute main function
    main()
