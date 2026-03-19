"""
Structured logging configuration.

This module provides centralized logging setup for the entire project.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from src.graphic_classifier.core.settings import LOG_LEVEL, LOG_FILE


def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    console_output: bool = True
) -> logging.Logger:
    """
    Configure logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file. If None, uses LOG_FILE from settings
        console_output: Whether to output logs to console
    
    Returns:
        Configured root logger
    """
    # Determine log level
    log_level = level or LOG_LEVEL
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Determine log file path
    file_path = log_file or LOG_FILE
    
    # Ensure log directory exists
    log_dir = Path(file_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Add file handler for system errors only
    error_log_path = "logs/system_errors.log"
    error_log_dir = Path(error_log_path).parent
    error_log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(error_log_path, encoding="utf-8")
    file_handler.setLevel(logging.ERROR)  # Apenas ERROR e CRITICAL
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    return root_logger


def setup_logger(
    name: Optional[str] = None,
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    console_output: bool = True
) -> logging.Logger:
    """Backward-compatible wrapper around ``setup_logging``.

    Args:
        name: Optional logger name to retrieve after configuring logging.
        level: Logging level override.
        log_file: Path override for the log file.
        console_output: Whether to emit logs to stdout.

    Returns:
        Requested logger instance (module-specific if ``name`` provided, otherwise root).
    """

    configured_logger = setup_logging(
        level=level,
        log_file=log_file,
        console_output=console_output,
    )

    if name:
        return get_logger(name)

    return configured_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (typically __name__ of the module)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


__all__ = ("setup_logging", "setup_logger", "get_logger")


# Initialize logging on module import
setup_logging()

