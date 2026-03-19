"""
Authentication and Email Validation Module

Provides corporate email validation and access control for the application.
"""

from .email_validator import EmailValidator, EmailValidationResult

__all__ = [
    "EmailValidator",
    "EmailValidationResult",
]
