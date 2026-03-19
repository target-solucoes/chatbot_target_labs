"""
Email Authentication Component for Streamlit

Provides UI components for email validation and access control.
"""

import streamlit as st
from typing import Optional, Callable
from src.auth import EmailValidator, EmailValidationResult
from streamlit_app.session_activity_tracker import mark_session_closed


class EmailAuthComponent:
    """
    Streamlit component for email authentication.

    Handles the UI for email input, validation, and access control.
    """

    # Session state keys
    SESSION_KEY_EMAIL = "authenticated_email"
    SESSION_KEY_IS_AUTHENTICATED = "is_authenticated"
    SESSION_KEY_AUTH_ATTEMPTED = "auth_attempted"

    @classmethod
    def initialize_session(cls) -> None:
        """Initialize session state variables for authentication"""
        if cls.SESSION_KEY_EMAIL not in st.session_state:
            st.session_state[cls.SESSION_KEY_EMAIL] = None

        if cls.SESSION_KEY_IS_AUTHENTICATED not in st.session_state:
            st.session_state[cls.SESSION_KEY_IS_AUTHENTICATED] = False

        if cls.SESSION_KEY_AUTH_ATTEMPTED not in st.session_state:
            st.session_state[cls.SESSION_KEY_AUTH_ATTEMPTED] = False

    @classmethod
    def is_authenticated(cls) -> bool:
        """
        Check if user is authenticated.

        Returns:
            True if user has validated their email, False otherwise
        """
        return st.session_state.get(cls.SESSION_KEY_IS_AUTHENTICATED, False)

    @classmethod
    def get_authenticated_email(cls) -> Optional[str]:
        """
        Get the authenticated email address.

        Returns:
            Email address if authenticated, None otherwise
        """
        if cls.is_authenticated():
            return st.session_state.get(cls.SESSION_KEY_EMAIL)
        return None

    @classmethod
    def logout(cls) -> None:
        """Clear authentication and close session log with complete memory cleanup."""
        import logging

        logger = logging.getLogger(__name__)
        session_id = st.session_state.get("session_id")

        # ========== CLOSE SESSION LOG ==========
        try:
            if "session_logger" in st.session_state:
                session_logger = st.session_state["session_logger"]
                session_logger.close_session()
                logger.info(f"Session {session_id} closed in database")
        except Exception as e:
            logger.warning(f"Failed to close session: {e}")
        # =======================================

        # Mark session as closed in activity tracker
        if session_id:
            mark_session_closed(session_id)
            logger.info(f"Session {session_id} removed from activity tracker")

        # ========== COMPREHENSIVE MEMORY CLEANUP ==========
        # Keys to preserve across logout
        preserve_keys = {
            cls.SESSION_KEY_EMAIL,
            cls.SESSION_KEY_IS_AUTHENTICATED,
            cls.SESSION_KEY_AUTH_ATTEMPTED,
        }

        # Clear all session state except preserved keys
        keys_to_delete = [
            key for key in st.session_state.keys() if key not in preserve_keys
        ]
        for key in keys_to_delete:
            del st.session_state[key]

        # Reset authentication state
        st.session_state[cls.SESSION_KEY_EMAIL] = None
        st.session_state[cls.SESSION_KEY_IS_AUTHENTICATED] = False
        st.session_state[cls.SESSION_KEY_AUTH_ATTEMPTED] = False

        logger.info(
            f"Memory cleanup complete: {len(keys_to_delete)} keys deleted, "
            f"{len(preserve_keys)} keys preserved"
        )
        # ==================================================

    @classmethod
    def render_auth_screen(cls) -> bool:
        """
        Render the authentication screen.

        Returns:
            True if authentication successful, False if still pending
        """
        from pathlib import Path

        cls.initialize_session()

        # If already authenticated, return True
        if cls.is_authenticated():
            return True

        # Add vertical centering wrapper
        st.markdown('<div class="vertical-center-wrapper">', unsafe_allow_html=True)

        # Logo Section
        logo_path = Path("streamlit_app/images/target_solucoes_comerciais_logo.png")
        if logo_path.exists():
            c1, c2, c3 = st.columns([1, 0.8, 1])
            with c2:
                # Logo with increased size
                st.image(str(logo_path), width=350)

        # Titles using markdown
        st.markdown(
            """
            <div class="auth-card">
                <h1 class="auth-title">Inteligência Artificial para Análise de Dados</h1>
                <p class="auth-subtitle">Sistema avançado de análise conversacional com IA para insights comerciais estratégicos</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Login Form
        # Using columns to center and constrain width of inputs
        col1, col2, col3 = st.columns([1, 1.2, 1])

        with col2:
            # Email input
            email = st.text_input(
                "E-mail Corporativo",
                placeholder="seu.nome@empresa.com.br",
                key="email_input",
                label_visibility="visible",
            )

            st.markdown("<div style='height: 15px'></div>", unsafe_allow_html=True)

            # Submit button
            submitted = st.button(
                "ACESSAR SISTEMA",
                type="primary",
                use_container_width=True,
                key="auth_submit",
            )

            # Handle submission
            if submitted:
                st.session_state[cls.SESSION_KEY_AUTH_ATTEMPTED] = True

                if not email:
                    st.error("⚠️ Por favor, insira um e-mail.")
                else:
                    # Validate email using backend
                    result = EmailValidator.validate(email)

                    if result.is_valid:
                        # Authentication successful
                        st.session_state[cls.SESSION_KEY_EMAIL] = result.email
                        st.session_state[cls.SESSION_KEY_IS_AUTHENTICATED] = True
                        st.success("✅ E-mail validado com sucesso!")
                        st.rerun()
                    else:
                        # Show specific error message
                        cls._show_validation_error(result)

        # Close vertical centering wrapper
        st.markdown("</div>", unsafe_allow_html=True)

        return False

    @classmethod
    def _show_validation_error(cls, result: EmailValidationResult) -> None:
        """Display validation error based on error type"""
        if result.error_type == "format":
            st.error("❌ **Formato de e-mail inválido**")
        elif result.error_type == "public_domain":
            st.error("🚫 **E-mail público não permitido**")
        else:
            st.error(f"❌ **Erro de validação**\n\n{result.error_message}")

    @classmethod
    def render_auth_header(cls) -> None:
        """
        Render authentication status in the sidebar.

        Shows the authenticated email and logout option.
        """
        if cls.is_authenticated():
            # Inject sidebar styles
            cls._inject_sidebar_styles()

            email = cls.get_authenticated_email()

            with st.sidebar:
                st.markdown("---")
                st.markdown("### 👤 Usuário Autenticado")
                # Display email with custom styling (no white background)
                st.markdown(f"**E-mail:**")
                st.markdown(
                    f"""
                    <div class="user-email-display">
                        {email}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                if st.button("🚪 Sair", use_container_width=True, type="secondary"):
                    cls.logout()
                    st.rerun()

                st.markdown("---")

    @classmethod
    def require_authentication(cls, callback: Optional[Callable] = None) -> bool:
        """
        Require authentication before allowing access.

        This method should be called at the beginning of the app.
        If user is not authenticated, it shows the auth screen.

        Args:
            callback: Optional callback to execute after successful authentication

        Returns:
            True if authenticated, False if authentication screen is shown
        """
        cls.initialize_session()

        if not cls.is_authenticated():
            # Add custom CSS for authentication screen
            cls._inject_auth_styles()

            # Show authentication screen
            is_auth = cls.render_auth_screen()

            if is_auth and callback:
                callback()

            return is_auth

        return True

    @staticmethod
    def _get_image_base64(path: str) -> str:
        """Read image and return base64 string"""
        import base64

        try:
            with open(path, "rb") as f:
                data = f.read()
            return base64.b64encode(data).decode()
        except:
            return ""

    @classmethod
    def _inject_auth_styles(cls) -> None:
        """Inject custom CSS for authentication screen"""

        # Load background image
        img_path = "streamlit_app/images/Dark_Blue_Futuristic.png"
        img_base64 = cls._get_image_base64(img_path)

        bg_css = ""
        if img_base64:
            bg_css = f"""
            .stApp {{
                background-image: url("data:image/png;base64,{img_base64}");
                background-size: 100% 100%;
                background-position: center;
                background-repeat: no-repeat;
                background-attachment: fixed;
            }}
            """

        st.markdown(
            f"""
            <style>

            {bg_css}

            /* GLOBAL AUTH PAGE CLEANUP */
            header {{visibility: hidden;}}
            footer {{visibility: hidden;}}
            .stApp > header {{display: none;}}
            #MainMenu {{visibility: hidden;}}

            /* Force full height on all containers */
            html, body {{
                height: 100% !important;
                margin: 0 !important;
                padding: 0 !important;
            }}

            .stApp {{
                height: 100vh !important;
            }}

            [data-testid="stAppViewContainer"] {{
                height: 100vh !important;
            }}

            /* Main container - make it relative for absolute positioning */
            section.main {{
                position: relative !important;
                height: 100vh !important;
                padding: 0 !important;
                margin: 0 !important;
            }}

            /* Block container - use absolute positioning for perfect centering */
            .block-container {{
                position: absolute !important;
                top: 40% !important;
                left: 50% !important;
                transform: translate(-50%, -50%) !important;
                padding: 1rem !important;
                margin: 0 !important;
                max-width: 1000px !important;
                width: 100% !important;
            }}

            /* Vertical center wrapper */
            .vertical-center-wrapper {{
                width: 100%;
            }}

            /* Remove default spacing from element container */
            .element-container {{
                margin: 0.5rem 0 !important;
            }}

            /* Column containers for form elements */
            div[data-testid="column"] {{
                display: inline-block !important;
                flex-direction: column !important;
                justify-content: center !important;
                align-items: center !important;
            }}

            /* Streamlit columns wrapper */
            [data-testid="stHorizontalBlock"] {{
                gap: 0 !important;
            }}

            /* Logo Styling */
            div[data-testid="stImage"] > img {{
                margin: -20px auto 10px auto !important; /* move 20px para cima */
                display: block;
            }}

            /* Modern Glassmorphism Card */
            .auth-card {{
                background: rgba(100, 120, 160, 0.01);
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.15);
                border-radius: 20px;
                padding: 1rem;
                margin: 0 auto 1rem auto !important;
                text-align: center;
                box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
                max-width: 900px;
            }}

            .auth-title {{
                color: #ffffff !important;
                font-family: 'Segoe UI', sans-serif;
                font-size: 2rem !important;
                font-weight: 300 !important;
                margin-bottom: 0.75rem !important;
                letter-spacing: 1px;
                text-shadow: 0 2px 4px rgba(0,0,0,0.5);
            }}

            .auth-subtitle {{
                color: rgba(255, 255, 255, 0.85) !important;
                font-size: 1.15rem !important;
                font-weight: 300 !important;
                margin: -1rem 0 0 0 !important;
                line-height: 1.6;
            }}

            /* Form Style Overrides */
            .stTextInput {{
                margin-bottom: 0.5rem !important;
            }}

            .stTextInput label {{
                color: rgba(255, 255, 255, 0.95) !important;
                font-size: 1.05rem !important;
                margin-bottom: 0.75rem !important;
                font-weight: 500 !important;
            }}

            .stTextInput input {{
                background-color: rgba(10, 10, 10, 0.9) !important;
                color: #ffffff !important;
                border: 1px solid rgba(255, 255, 255, 0.3) !important;
                border-radius: 8px !important;
                height: 40px !important;
                line-height: 40px !important;
                font-size: 1.05rem !important;
                padding: 0 1.25rem !important;
                vertical-align: middle !important;
                display: flex !important;
                align-items: center !important;
                box-sizing: border-box !important;
            }}

            .stTextInput input::placeholder {{
                color: rgba(255, 255, 255, 0.6) !important;
            }}

            .stTextInput input:focus {{
                border-color: #e74c3c !important;
                background-color: rgba(0, 0, 0, 0.5) !important;
                box-shadow: 0 0 0 2px rgba(231, 76, 60, 0.3) !important;
                outline: none !important;
            }}

            /* Ensure text is visible when typing */
            .stTextInput input:not(:placeholder-shown) {{
                color: #ffffff !important;
                background-color: rgba(0, 0, 0, 0.4) !important;
            }}

            .stButton {{
                margin-top: 0.5rem !important;
            }}

            .stButton button {{
                background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%) !important;
                height: 56px !important;
                border-radius: 8px !important;
                font-weight: 500 !important;
                font-size: 1.05rem !important;
                letter-spacing: 1px;
                margin-top: 0.75rem !important;
                border: none !important;
                transition: all 0.3s ease !important;
            }}

            .stButton button:hover {{
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(231, 76, 60, 0.5) !important;
            }}

            .stButton button:active {{
                transform: translateY(0px);
            }}

            /* Alert/Error message styling - Universal */
            .stAlert {{
                border-radius: 8px !important;
                margin-top: 1.25rem !important;
                padding: 1.1rem 1.3rem !important;
                font-weight: 500 !important;
                font-size: 1rem !important;
            }}

            /* Success alerts - Green solid background */
            div[data-testid="stSuccessIcon"],
            .stSuccess,
            [class*="success"] {{
                background-color: rgba(34, 139, 34, 0.95) !important;
                border-left: 5px solid #228B22 !important;
                color: #ffffff !important;
            }}

            /* Error alerts - Red solid background */
            div[data-testid="stErrorIcon"],
            .stError,
            [class*="error"] {{
                background-color: rgba(220, 53, 69, 0.95) !important;
                border-left: 5px solid #c82333 !important;
                color: #ffffff !important;
            }}

            /* Warning alerts - Orange solid background */
            div[data-testid="stWarningIcon"],
            .stWarning,
            [class*="warning"] {{
                background-color: rgba(255, 140, 0, 0.95) !important;
                border-left: 5px solid #ff8c00 !important;
                color: #ffffff !important;
            }}

            /* Info alerts - Blue solid background */
            div[data-testid="stInfoIcon"],
            .stInfo,
            [class*="info"] {{
                background-color: rgba(23, 162, 184, 0.95) !important;
                border-left: 5px solid #138496 !important;
                color: #ffffff !important;
            }}

            /* Force text color in all alerts */
            .stAlert *,
            .stSuccess *,
            .stError *,
            .stWarning *,
            .stInfo *,
            [data-testid="stMarkdownContainer"] p {{
                color: #ffffff !important;
            }}

            /* Specific override for alert containers */
            div[data-baseweb="notification"] {{
                background-color: inherit !important;
                border-radius: 8px !important;
            }}

            div[data-baseweb="notification"] > div {{
                color: #ffffff !important;
            }}

            </style>
            """,
            unsafe_allow_html=True,
        )

    @classmethod
    def _inject_sidebar_styles(cls) -> None:
        """Inject custom CSS for authenticated sidebar display"""
        st.markdown(
            """
            <style>
            /* Remove white background from code blocks in sidebar */
            [data-testid="stSidebar"] code {
                background-color: transparent !important;
                color: inherit !important;
                padding: 0 !important;
                border: none !important;
            }

            /* Custom email display styling with red border */
            .user-email-display {
                display: block !important;
                background-color: rgba(40, 44, 52, 0.95) !important;
                border: 2px solid #e74c3c !important;
                border-radius: 6px !important;
                padding: 0.6rem 0.8rem !important;
                font-family: 'Courier New', monospace !important;
                font-size: 0.875rem !important;
                color: #e0e0e0 !important;
                word-break: break-all !important;
                margin-top: 0.5rem !important;
                margin-bottom: 0.75rem !important;
                width: 100% !important;
                box-sizing: border-box !important;
            }

            /* Ensure parent container doesn't override */
            [data-testid="stSidebar"] .user-email-display {
                background-color: rgba(40, 44, 52, 0.95) !important;
                border: 2px solid #e74c3c !important;
            }

            /* Remove white backgrounds from all sidebar elements */
            [data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] code {
                background: transparent !important;
                border: none !important;
            }

            [data-testid="stSidebar"] pre {
                background-color: rgba(40, 44, 52, 0.8) !important;
                border: 1px solid rgba(255, 255, 255, 0.1) !important;
            }

            /* Force visibility */
            [data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] .user-email-display {
                visibility: visible !important;
                opacity: 1 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
