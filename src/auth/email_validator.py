"""
Email Validator Module

Validates corporate email addresses and blocks public email domains.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class EmailValidationResult:
    """Result of email validation"""

    is_valid: bool
    email: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None  # 'format', 'public_domain', 'invalid'


class EmailValidator:
    """
    Validates corporate email addresses.

    This validator ensures that only corporate emails are accepted,
    blocking common public email domains like gmail.com, outlook.com, etc.
    """

    # Common public email domains that should be blocked
    PUBLIC_DOMAINS = {
        # Google
        "gmail.com",
        "googlemail.com",
        # Microsoft
        "outlook.com",
        "hotmail.com",
        "live.com",
        "msn.com",
        # Yahoo
        "yahoo.com",
        "yahoo.com.br",
        "ymail.com",
        # Apple
        "icloud.com",
        "me.com",
        "mac.com",
        # Other popular services
        "aol.com",
        "protonmail.com",
        "proton.me",
        "mail.com",
        "gmx.com",
        "zoho.com",
        "yandex.com",
        "tutanota.com",
        # Brazilian providers
        "uol.com.br",
        "bol.com.br",
        "terra.com.br",
        "ig.com.br",
        "globo.com",
        "globomail.com",
        # Temporary/disposable
        "mailinator.com",
        "guerrillamail.com",
        "temp-mail.org",
        "10minutemail.com",
        "throwaway.email",
    }

    # Regex pattern for email validation (RFC 5322 simplified)
    EMAIL_PATTERN = re.compile(
        r"^[a-zA-Z0-9][a-zA-Z0-9._%+-]*@[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}$"
    )

    @classmethod
    def validate(cls, email: str) -> EmailValidationResult:
        """
        Validate a corporate email address.

        Args:
            email: Email address to validate

        Returns:
            EmailValidationResult with validation status and details
        """
        # Normalize email
        email = email.strip().lower()

        # Check if empty
        if not email:
            return EmailValidationResult(
                is_valid=False,
                error_message="E-mail não pode estar vazio",
                error_type="invalid",
            )

        # Check format
        if not cls.EMAIL_PATTERN.match(email):
            return EmailValidationResult(
                is_valid=False,
                email=email,
                error_message="Formato de e-mail inválido",
                error_type="format",
            )

        # Extract domain
        domain = email.split("@")[1]

        # Check if it's a public domain
        if domain in cls.PUBLIC_DOMAINS:
            return EmailValidationResult(
                is_valid=False,
                email=email,
                error_message=(
                    "E-mail público não permitido. "
                    "Por favor, utilize um e-mail corporativo."
                ),
                error_type="public_domain",
            )

        # Email is valid
        return EmailValidationResult(
            is_valid=True, email=email, error_message=None, error_type=None
        )

    @classmethod
    def is_corporate_email(cls, email: str) -> bool:
        """
        Quick check if email is corporate (not public).

        Args:
            email: Email address to check

        Returns:
            True if corporate email, False otherwise
        """
        result = cls.validate(email)
        return result.is_valid

    @classmethod
    def add_public_domain(cls, domain: str) -> None:
        """
        Add a domain to the public domains blocklist.

        Args:
            domain: Domain to add (e.g., 'example.com')
        """
        cls.PUBLIC_DOMAINS.add(domain.lower().strip())

    @classmethod
    def remove_public_domain(cls, domain: str) -> None:
        """
        Remove a domain from the public domains blocklist.

        Args:
            domain: Domain to remove
        """
        cls.PUBLIC_DOMAINS.discard(domain.lower().strip())

    @classmethod
    def get_public_domains(cls) -> set:
        """
        Get the current list of blocked public domains.

        Returns:
            Set of public domain strings
        """
        return cls.PUBLIC_DOMAINS.copy()
