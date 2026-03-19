# -*- coding: utf-8 -*-
"""
Streamlit Chatbot Application for Analytics Pipeline

Interactive chatbot interface that integrates with the multi-agent
formatter pipeline to provide real-time analytics and visualizations.
"""

import streamlit as st
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
import pandas as pd

from streamlit_app.session_state import SessionStateManager
from streamlit_app.pipeline_runner import StreamingPipelineRunner
from streamlit_app.progressive_display import (
    ProgressiveRenderer,
    StreamingDisplayManager,
)
from streamlit_app.display_components import (
    render_complete_response,
)
from streamlit_app.email_auth import EmailAuthComponent
from streamlit_app.session_timeout_manager import SessionTimeoutManager
from streamlit_app.components.session_monitor import (
    SessionMonitorComponent,
    SessionMonitorConfig,
)
from streamlit_app.session_activity_tracker import (
    record_client_heartbeat,
    register_session_activity,
)
from streamlit_app.components.alias_manager import get_alias_manager
from src.analytics_executor.execution.filter_normalizer import normalize_filters
from src.non_graph_executor.tools.metadata_cache import MetadataCache
from src.shared_lib.utils.logger import get_logger
from src.shared_lib.core.integrity_validator import (
    validate_dataset_integrity,
    DatasetIntegrityError,
)
from src.shared_lib.core.config import DEVELOPER_MODE

logger = get_logger(__name__)


SESSION_TIMEOUT_MINUTES = 1440  # 24 hours


@st.cache_resource
def _validate_dataset_on_startup() -> bool:
    """Run dataset integrity validation once per app lifetime.

    Uses @st.cache_resource so validation executes only on first load,
    not on every Streamlit rerun.

    Returns:
        True if validation passed successfully.

    Raises:
        DatasetIntegrityError: If critical mismatches are found.
    """
    logger.info("[Startup] Running dataset integrity validation...")
    warnings = validate_dataset_integrity(strict=True)
    if warnings:
        for w in warnings:
            logger.warning("[Startup] %s", w)
    logger.info("[Startup] Dataset integrity validation passed.")
    return True


# Page Configuration
st.set_page_config(
    page_title="Analytics Chatbot - Target Solucoes",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _setup_page_styling():
    """Aplica CSS customizado para layout profissional"""
    st.markdown(
        """
    <style>
    /* ===== CRITICAL: REMOVE ALL STREAMLIT DEFAULT SPACING - NUCLEAR OPTION ===== */
    .main,
    .main > div,
    .block-container,
    section.main,
    section.main > div,
    section[data-testid="stAppViewContainer"],
    section[data-testid="stAppViewContainer"] > div,
    div[data-testid="stAppViewBlockContainer"],
    div.appview-container > section,
    div.appview-container > section > div {
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
        margin-top: 0rem !important;
    }

    /* Remove top padding but keep bottom padding for content */
    .block-container {
        padding-top: 0rem !important;
        padding-bottom: 2rem !important;
        margin-top: 0rem !important;
    }

    /* Header Styling - FORCE TO TOP WITH NEGATIVE MARGIN */
    .header-container {
        background: linear-gradient(135deg, #1a2332 0%, #2d3e50 100%);
        padding: 2rem 1rem;
        margin: -8rem -4rem 1.5rem -4rem;
        border-radius: 20px 20px 20px 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        width: calc(100% + 8rem);
        position: relative;
        top: -1rem;
    }

    .app-title {
        color: white !important;
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 2.5rem;
        font-weight: 300;
        margin: 0 auto;
        letter-spacing: 2px;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        text-align: center !important;
    }

    .app-subtitle {
        color: white !important;
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 1rem;
        font-weight: 300;
        margin: 0.5rem auto 0 auto;
        letter-spacing: 1px;
        opacity: 0.95;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
        text-align: center !important;
    }

    .app-description {
        color: rgba(255,255,255,0.85) !important;
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 0.9rem;
        font-weight: 300;
        margin: 1rem auto 0 auto !important;
        padding: 0 1rem;
        max-width: 700px;
        line-height: 1.7;
        text-align: center !important;
        display: block;
    }

    .feature-icons {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 2rem;
        margin-top: 1.5rem;
        flex-wrap: wrap;
    }

    .feature-item {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        color: rgba(255,255,255,0.7);
        font-size: 0.8rem;
        font-weight: 300;
        text-align: center;
    }

    .feature-icon {
        font-size: 1.5rem;
        margin-bottom: 0.5rem;
        opacity: 0.8;
    }

    /* Chat Container Styling */
    .chat-main-container {
        display: flex;
        flex-direction: column;
        margin: 2rem 0;
    }

    .chat-messages-container {
        padding: 1rem;
        margin-bottom: 1rem;
        border-radius: 15px;
        border: 1px solid var(--secondary-background-color);
    }

    .chat-input-container {
        padding: 1.5rem 0;
        margin-top: 1rem;
        border-top: 1px solid var(--secondary-background-color);
    }

    /* Chat Message Styling - Dark mode friendly */
    .stChatMessage {
        border-radius: 15px;
        padding: 1.2rem;
        margin: 0.8rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border: 1px solid var(--secondary-background-color);
    }

    .stChatMessage[data-testid="user-message"] {
        background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%) !important;
        color: white !important;
        margin-left: 2rem;
    }

    .stChatMessage[data-testid="assistant-message"] {
        border-left: 4px solid #e74c3c;
        margin-right: 2rem;
    }

    /* Chat Input Styling - Dark mode friendly */
    .stChatInputContainer {
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        background: transparent;
    }

    .stChatInput > div {
        border-radius: 25px !important;
        border: 2px solid #e74c3c !important;
    }

    .stChatInput input {
        border: none !important;
        font-size: 1rem !important;
        padding: 1rem 1.5rem !important;
    }

    /* Welcome message styling - Dark mode friendly */
    .welcome-message {
        text-align: center;
        padding: 3rem 2rem;
        font-style: italic;
        border-radius: 15px;
        margin: 2rem 0;
        border: 2px dashed var(--secondary-background-color);
    }

    .welcome-message h3 {
        color: #e74c3c;
        margin-bottom: 1rem;
    }

    /* Button Styling */
    .stButton > button {
        background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
        color: white;
        border: none;
        border-radius: 20px;
        padding: 0.4rem 0.8rem;
        font-weight: 400;
        font-size: 0.85rem;
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(108, 117, 125, 0.3);
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #5a6268 0%, #495057 100%);
        transform: translateY(-1px);
        box-shadow: 0 3px 12px rgba(108, 117, 125, 0.4);
    }

    .stButton > button:active {
        transform: translateY(0px);
    }

    /* Debug mode toggle styling */
    .stToggle > div {
        background-color: transparent !important;
    }

    .stToggle > div > div {
        background-color: #f0f0f0 !important;
        border-radius: 20px !important;
    }

    .stToggle > div > div[data-checked="true"] {
        background-color: #e74c3c !important;
    }

    /* Debug section styling */
    .debug-section {
        background-color: rgba(231, 76, 60, 0.05);
        border: 1px solid rgba(231, 76, 60, 0.2);
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
    }

    .debug-title {
        color: #e74c3c;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }

    /* Filter Management Styling */
    .filter-checkbox {
        margin-right: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }

    .filter-item {
        display: flex;
        align-items: center;
        margin-bottom: 0.5rem;
        padding: 0.25rem 0;
    }

    .filter-text {
        flex: 1;
        margin-left: 0.5rem;
    }

    .disabled-filter {
        opacity: 0.6;
        text-decoration: line-through;
        color: var(--text-color-light, #666);
    }

    .enabled-filter {
        opacity: 1;
        text-decoration: none;
    }

    /* Sidebar filter management styling */
    .stSidebar .stCheckbox {
        margin-bottom: 0.25rem !important;
    }

    .stSidebar .stCheckbox > div {
        margin-bottom: 0 !important;
    }

    .filter-category-header {
        font-weight: bold;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        border-bottom: 1px solid var(--secondary-background-color);
        padding-bottom: 0.25rem;
    }

    .filter-status-info {
        font-size: 0.85rem;
        font-style: italic;
        padding: 0.5rem;
        margin-top: 0.5rem;
        border-radius: 5px;
        background: var(--secondary-background-color);
    }

    .reactivate-button {
        width: 100% !important;
        margin-top: 0.5rem !important;
    }

    /* Interactive Filter Controls */
    .filter-item {
        transition: background-color 0.2s ease;
    }

    .filter-item:hover {
        background-color: #d1dae3 !important;
    }

    /* Remove button styling */
    button[kind="secondary"] {
        background-color: #cbd5e1 !important;
        color: #475569 !important;
        border: 1px solid #94a3b8 !important;
        border-radius: 4px !important;
        padding: 0.3rem 0.5rem !important;
        font-size: 1.1rem !important;
        font-weight: normal !important;
        line-height: 1 !important;
        min-width: 32px !important;
        height: 32px !important;
        transition: background-color 0.2s ease !important;
    }

    button[kind="secondary"]:hover {
        background-color: #94a3b8 !important;
        color: #1e293b !important;
    }

    button[kind="secondary"]:active {
        background-color: #64748b !important;
        color: white !important;
    }

    /* Modern minimalist enhancements */
    .stApp {
        background-color: var(--background-color);
    }

    /* ===========================================
       GLOBAL VARIABLES
       =========================================== */
    :root {
        --sidebar-bg: #f8f9fa;
        --sidebar-text: #1a202c;
        --sidebar-text-strong: #000000;
        --sidebar-border: #dee2e6;
        --accent-red: #e74c3c;
        --sidebar-hover: rgba(231, 76, 60, 0.08);
    }

    /* ===========================================
       SIDEBAR - Light Mode
       =========================================== */
    .stSidebar {
        background: var(--sidebar-bg) !important;
        border-right: 2px solid var(--sidebar-border);
    }

    .stSidebar > div {
        padding-top: 2rem;
    }

    /* Sidebar headings */
    .stSidebar h2,
    .stSidebar h3 {
        color: var(--sidebar-text) !important;
        font-weight: 700 !important;
        margin-bottom: 1rem !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 2px solid var(--accent-red) !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    .stSidebar h2 {
        font-size: 1.1rem !important;
    }

    .stSidebar h3 {
        font-size: 0.95rem !important;
    }

    /* Sidebar text */
    .stSidebar p,
    .stSidebar div,
    .stSidebar span,
    .stSidebar label,
    .stSidebar em,
    .stSidebar .stMarkdown,
    .stSidebar .stMarkdown *,
    .stSidebar div[data-testid="stMarkdownContainer"],
    .stSidebar div[data-testid="stMarkdownContainer"] * {
        color: var(--sidebar-text) !important;
        font-weight: 600 !important;
    }

    .stSidebar strong {
        color: var(--sidebar-text-strong) !important;
        font-weight: 800 !important;
    }

    .stSidebar .stCheckbox label,
    .stSidebar .stToggle label {
        color: var(--sidebar-text) !important;
        font-weight: 600 !important;
    }

    /* Sidebar expander - NO WHITE BACKGROUND */
    .stSidebar .stExpander {
        background: transparent !important;
        border: none !important;
    }

    .stSidebar .stExpander summary {
        color: var(--sidebar-text) !important;
        font-weight: 700 !important;
        background: transparent !important;
        border: 1px solid var(--sidebar-border) !important;
        border-radius: 8px !important;
        padding: 0.75rem !important;
        margin: 0.5rem 0 !important;
        transition: all 0.2s ease !important;
    }

    .stSidebar .stExpander summary:hover {
        background: var(--sidebar-hover) !important;
        border-color: var(--accent-red) !important;
    }

    .stSidebar .stExpander > div:last-child {
        background: transparent !important;
        border: none !important;
        padding: 0.5rem 0 !important;
    }

    /* Email display - HIGH CONTRAST - Multiple selectors for specificity */
    .stSidebar code,
    .stSidebar .stMarkdown code,
    .stSidebar div[data-testid="stMarkdownContainer"] code,
    section[data-testid="stSidebar"] code {
        background-color: #1a202c !important;
        color: #ffffff !important;
        padding: 0.4rem 0.8rem !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
        border: 2px solid #e74c3c !important;
        display: inline-block !important;
        margin: 0.25rem 0 !important;
        box-shadow: 0 2px 8px rgba(231, 76, 60, 0.2) !important;
    }

    /* Enhanced expander styling - Main content */
    .stExpander {
        border: 1px solid var(--sidebar-border, #e0e0e0);
        border-radius: 8px;
        margin: 0.5rem 0;
        background: transparent;
    }

    .stExpander > div > div {
        background: transparent;
        border-radius: 8px;
    }

    /* Code block improvements */
    .stCodeBlock {
        background-color: #f8f9fa;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    }

    /* Processing Spinner Styling */
    .stSpinner > div {
        border-top-color: #e74c3c !important;
        animation: spinner-rotation 1s linear infinite !important;
    }

    @keyframes spinner-rotation {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }

    /* Processing message styling */
    .stSpinner + div {
        color: #2c3e50;
        font-weight: 500;
        font-size: 0.95rem;
        text-align: center;
        margin-top: 0.5rem;
        animation: pulse-fade 2s ease-in-out infinite;
    }

    @keyframes pulse-fade {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }

    /* Custom processing status box styling */
    div[style*="linear-gradient(135deg, #fff3e0"] {
        animation: processing-pulse 2s ease-in-out infinite !important;
        transition: all 0.3s ease !important;
    }

    @keyframes processing-pulse {
        0%, 100% { 
            opacity: 1; 
            transform: scale(1);
        }
        50% { 
            opacity: 0.9;
            transform: scale(0.995);
        }
    }

    /* Metric improvements */
    .stMetric {
        background-color: rgba(255,255,255,0.8);
        border-radius: 10px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border: 1px solid #e0e0e0;
    }

    /* Success message styling */
    .stSuccess {
        background-color: rgba(46, 204, 113, 0.1);
        border: 1px solid #2ecc71;
        border-radius: 8px;
        color: #27ae60;
    }

    /* Warning message styling */
    .stWarning {
        background-color: rgba(241, 196, 15, 0.1);
        border: 1px solid #f1c40f;
        border-radius: 8px;
        color: #f39c12;
    }

    /* Error message styling */
    .stError {
        background-color: rgba(231, 76, 60, 0.1);
        border: 1px solid #e74c3c;
        border-radius: 8px;
        color: #c0392b;
    }

    /* Modern card styling for containers */
    .card-container {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        border: 1px solid #e0e0e0;
    }

    /* Responsive adjustments */
    @media (max-width: 768px) {
        .app-title {
            font-size: 2rem;
        }
        .feature-icons {
            gap: 1rem;
        }
        .header-container {
            padding: 1.5rem 1rem;
        }
        .stChatMessage[data-testid="user-message"] {
            margin-left: 0.5rem;
        }
        .stChatMessage[data-testid="assistant-message"] {
            margin-right: 0.5rem;
        }
    }

    /* Sidebar separator - simplified */
    .stSidebar hr {
        border: none !important;
        border-top: 1px solid var(--sidebar-border) !important;
        margin: 1.2rem 0 !important;
        opacity: 0.6;
    }

    /* Fixed logo in sidebar */
    .sidebar-logo-container {
        position: sticky !important;
        top: 0 !important;
        z-index: 999 !important;
        background: var(--sidebar-bg) !important;
        padding: 1rem 0 1rem 0 !important;
        margin-bottom: 1rem !important;
    }

    .sidebar-logo-container img {
        max-width: 80% !important;
        height: auto !important;
        margin: 0 auto !important;
        display: block !important;
    }

    /* Sidebar buttons */
    .stSidebar .stButton button {
        font-weight: 600 !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
    }

    .stSidebar .stButton button[type="primary"] {
        background: linear-gradient(135deg, #c2410c 0%, #9a3412 100%) !important;
        color: white !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(194, 65, 12, 0.3) !important;
    }

    .stSidebar .stButton button[type="primary"]:hover {
        background: linear-gradient(135deg, #9a3412 0%, #7c2d12 100%) !important;
        box-shadow: 0 4px 12px rgba(194, 65, 12, 0.4) !important;
        transform: translateY(-1px) !important;
    }

    .stSidebar .stButton button[type="secondary"] {
        background: transparent !important;
        color: var(--sidebar-text) !important;
        border: 2px solid var(--sidebar-border) !important;
    }

    .stSidebar .stButton button[type="secondary"]:hover {
        background: var(--sidebar-hover) !important;
        border-color: var(--accent-red) !important;
    }

    /* FORÇA ESTILO EXPANDER NO BOTÃO GERENCIAR ALIASES - MÁXIMA PRIORIDADE */
    section[data-testid="stSidebar"] button[kind="secondary"],
    section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"],
    .stSidebar button[kind="secondary"] {
        background: transparent !important;
        color: var(--sidebar-text) !important;
        border: 1px solid var(--sidebar-border) !important;
        border-radius: 8px !important;
        padding: 0.75rem !important;
        font-weight: 600 !important;
        box-shadow: none !important;
    }

    section[data-testid="stSidebar"] button[kind="secondary"]:hover,
    section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"]:hover,
    .stSidebar button[kind="secondary"]:hover {
        background: rgba(231, 76, 60, 0.08) !important;
        border-color: #e74c3c !important;
    }

    /* Botão Gerenciar Aliases na sidebar - Estilo de expander */
    .stSidebar .stButton button[key="open_alias_manager"] {
        background: transparent !important;
        color: var(--sidebar-text) !important;
        border: 1px solid var(--sidebar-border) !important;
        border-radius: 8px !important;
        padding: 0.75rem !important;
        transition: all 0.2s ease !important;
        font-weight: 600 !important;
    }

    .stSidebar .stButton button[key="open_alias_manager"]:hover {
        background: var(--sidebar-hover) !important;
        border-color: var(--accent-red) !important;
    }

    /* ===========================================
       ALIAS MANAGER STYLING
       =========================================== */
    
    /* Alias expander styling */
    .alias-expander {
        border: 1px solid var(--secondary-background-color);
        border-radius: 8px;
        margin: 0.5rem 0;
        transition: all 0.2s ease;
    }

    .alias-expander:hover {
        border-color: #e74c3c;
        box-shadow: 0 2px 8px rgba(231, 76, 60, 0.1);
    }

    /* Alias item styling */
    .alias-item {
        display: flex;
        align-items: center;
        padding: 0.5rem;
        border-radius: 6px;
        background: var(--secondary-background-color);
        margin: 0.25rem 0;
        transition: background 0.2s ease;
    }

    .alias-item:hover {
        background: rgba(231, 76, 60, 0.1);
    }

    .alias-item code {
        background: transparent !important;
        padding: 0.2rem 0.5rem !important;
        border: none !important;
        font-size: 0.9rem !important;
    }

    /* Remove button in alias manager */
    button[key^="remove_"] {
        font-size: 1rem !important;
        padding: 0.2rem 0.4rem !important;
        min-width: auto !important;
        height: auto !important;
        border: none !important;
        background: transparent !important;
        transition: all 0.2s ease !important;
    }

    button[key^="remove_"]:hover {
        transform: scale(1.2) !important;
        color: #e74c3c !important;
    }

    /* Add alias button - Fundo branco e texto preto */
    button[key^="add_btn_"],
    div[data-testid="column"] button[kind="secondary"],
    .stButton button[kind="secondary"] {
        background: white !important;
        color: #1a202c !important;
        border: 2px solid #e0e0e0 !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1) !important;
    }

    button[key^="add_btn_"]:hover,
    div[data-testid="column"] button[kind="secondary"]:hover,
    .stButton button[kind="secondary"]:hover {
        background: #f8f9fa !important;
        border-color: #bdc3c7 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 3px 12px rgba(0, 0, 0, 0.15) !important;
    }

    /* Back to chat button */
    button[key="back_to_chat"],
    div button[kind="primary"] {
        background: white !important;
        color: #1a202c !important;
        border: 2px solid #e0e0e0 !important;
        font-size: 1rem !important;
        font-weight: 600 !important;
        padding: 0.6rem 1.2rem !important;
        margin-bottom: 1rem !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1) !important;
    }

    button[key="back_to_chat"]:hover,
    div button[kind="primary"]:hover {
        background: #f8f9fa !important;
        border-color: #bdc3c7 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15) !important;
    }

    /* Resetar e Salvar buttons - Fundo branco e texto preto */
    button[key="reset_all_aliases"],
    button[key="save_aliases"],
    div[data-testid="column"] button[key="reset_all_aliases"],
    div[data-testid="column"] button[key="save_aliases"],
    section button[key="reset_all_aliases"],
    section button[key="save_aliases"] {
        background: white !important;
        color: #1a202c !important;
        border: 2px solid #e0e0e0 !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1) !important;
    }

    button[key="reset_all_aliases"]:hover,
    button[key="save_aliases"]:hover,
    div[data-testid="column"] button[key="reset_all_aliases"]:hover,
    div[data-testid="column"] button[key="save_aliases"]:hover,
    section button[key="reset_all_aliases"]:hover,
    section button[key="save_aliases"]:hover {
        background: #f8f9fa !important;
        border-color: #bdc3c7 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 3px 12px rgba(0, 0, 0, 0.15) !important;
    }

    /* Remove caixas cinzas dos inputs no Alias Manager - UNIVERSAL */
    /* Remove container cinza do text_input */
    [data-testid="stTextInput"] > div:first-child,
    [data-testid="stTextInput"] > div,
    .stTextInput > div:first-child,
    .stTextInput > div > div:first-child {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        box-shadow: none !important;
    }

    /* Estilização dos inputs sem caixa cinza - DIRETO NO INPUT */
    [data-testid="stTextInput"] input,
    .stTextInput input,
    input[type="text"] {
        border-radius: 8px !important;
        border: 2px solid #e0e0e0 !important;
        padding: 0.6rem 1rem !important;
        background: transparent !important;
        box-shadow: none !important;
    }

    [data-testid="stTextInput"] input:focus,
    .stTextInput input:focus,
    input[type="text"]:focus {
        border-color: #e74c3c !important;
        box-shadow: 0 0 0 2px rgba(231, 76, 60, 0.1) !important;
        outline: none !important;
        background: transparent !important;
    }

    /* ===========================================
       DARK MODE - Sidebar
       =========================================== */
    @media (prefers-color-scheme: dark) {
        :root {
            --sidebar-bg: #1a202c;
            --sidebar-text: #f8f9fa;
            --sidebar-text-strong: #ffffff;
            --sidebar-border: #4a5a70;
            --sidebar-hover: rgba(231, 76, 60, 0.15);
        }

        .stSidebar {
            background: var(--sidebar-bg) !important;
            border-right: 2px solid var(--sidebar-border);
        }

        .stSidebar p,
        .stSidebar div,
        .stSidebar span,
        .stSidebar label,
        .stSidebar em,
        .stSidebar h2,
        .stSidebar h3,
        .stSidebar .stMarkdown,
        .stSidebar .stMarkdown *,
        .stSidebar div[data-testid="stMarkdownContainer"],
        .stSidebar div[data-testid="stMarkdownContainer"] * {
            color: var(--sidebar-text) !important;
        }

        .stSidebar strong {
            color: var(--sidebar-text-strong) !important;
        }

        .stSidebar h2,
        .stSidebar h3 {
            border-bottom-color: var(--accent-red) !important;
        }

        /* Email in dark mode - mantém mesmo padrão (preto com branco) */
        .stSidebar code,
        .stSidebar .stMarkdown code,
        .stSidebar div[data-testid="stMarkdownContainer"] code,
        section[data-testid="stSidebar"] code {
            background-color: #1a202c !important;
            color: #ffffff !important;
            border-color: #e74c3c !important;
            box-shadow: 0 2px 8px rgba(231, 76, 60, 0.3) !important;
        }

        /* Expander in dark mode */
        .stSidebar .stExpander summary {
            border-color: var(--sidebar-border) !important;
        }

        .stSidebar .stExpander summary:hover {
            background: var(--sidebar-hover) !important;
            border-color: var(--accent-red) !important;
        }

        .stSidebar hr {
            border-top-color: var(--sidebar-border) !important;
        }

        .stSidebar .stButton button[type="secondary"] {
            color: var(--sidebar-text) !important;
            border-color: var(--sidebar-border) !important;
        }

        .stSidebar .stButton button[type="secondary"]:hover {
            background: var(--sidebar-hover) !important;
            border-color: var(--accent-red) !important;
        }

        /* Botão Gerenciar Aliases no dark mode */
        .stSidebar .stButton button[key="open_alias_manager"] {
            color: var(--sidebar-text) !important;
            border-color: var(--sidebar-border) !important;
        }

        .stSidebar .stButton button[key="open_alias_manager"]:hover {
            background: var(--sidebar-hover) !important;
            border-color: var(--accent-red) !important;
        }

        .card-container {
            background: #34495e;
            border-color: var(--sidebar-border);
        }

        /* Fixed logo in dark mode */
        .sidebar-logo-container {
            background: var(--sidebar-bg) !important;
        }
    }

    /* ====================================================
       FORÇA ESTILOS VIA JAVASCRIPT (ÚLTIMA CAMADA)
       ==================================================== */
    </style>
    
    <script>
    // Força estilos nos botões após carregamento
    document.addEventListener('DOMContentLoaded', function() {
        // Estilo do botão Gerenciar Aliases
        const aliasBtn = document.querySelector('button[key="open_alias_manager"]');
        if (aliasBtn) {
            aliasBtn.style.cssText = 'background: transparent !important; border: 1px solid #dee2e6 !important; padding: 0.75rem !important;';
        }
        
        // Estilo do botão Limpar Conversa (cinza)
        const clearBtn = document.querySelector('button[key="sidebar_clear_conversation"]');
        if (clearBtn) {
            clearBtn.style.cssText = 'background: linear-gradient(135deg, #95a5a6 0%, #7f8c8d 100%) !important; color: white !important; border: none !important; font-weight: 600 !important; box-shadow: 0 2px 8px rgba(149, 165, 166, 0.3) !important;';
        }
        
        // Estilo dos botões Resetar e Salvar
        ['reset_all_aliases', 'save_aliases'].forEach(key => {
            const btn = document.querySelector(`button[key="${key}"]`);
            if (btn) {
                btn.style.cssText = 'background: white !important; color: #1a202c !important; border: 2px solid #e0e0e0 !important;';
            }
        });
        
        // Estilo dos botões Adicionar
        document.querySelectorAll('button[key^="add_btn_"]').forEach(btn => {
            btn.style.cssText = 'background: white !important; color: #1a202c !important; border: 2px solid #e0e0e0 !important;';
        });
        
        // Remove caixas cinzas dos inputs
        document.querySelectorAll('[data-testid="stTextInput"] > div').forEach(div => {
            div.style.cssText = 'background: transparent !important; border: none !important; padding: 0 !important;';
        });
    });
    
    // Observer para aplicar estilos em elementos dinâmicos
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            // Reaplica estilos quando novos elementos são adicionados
            document.querySelectorAll('button[key^="add_btn_"]').forEach(btn => {
                if (!btn.style.cssText.includes('white')) {
                    btn.style.cssText = 'background: white !important; color: #1a202c !important; border: 2px solid #e0e0e0 !important;';
                }
            });
            
            // Força estilo cinza no botão Limpar Conversa
            const clearBtn = document.querySelector('button[key="sidebar_clear_conversation"]');
            if (clearBtn && !clearBtn.style.cssText.includes('#95a5a6')) {
                clearBtn.style.cssText = 'background: linear-gradient(135deg, #95a5a6 0%, #7f8c8d 100%) !important; color: white !important; border: none !important; font-weight: 600 !important; box-shadow: 0 2px 8px rgba(149, 165, 166, 0.3) !important;';
            }
        });
    });
    
    observer.observe(document.body, { childList: true, subtree: true });
    </script>
    """,
        unsafe_allow_html=True,
    )


def get_metadata_cache(data_source: Optional[str]):
    """Return session-scoped MetadataCache instance for given dataset path."""
    if not data_source:
        return None

    cache_key = f"metadata_cache_{data_source}"
    session_id = st.session_state.get("session_id", "unknown")

    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = MetadataCache(data_source)
            logger.info(
                f"[Session {session_id}] Created session-scoped MetadataCache for {data_source}"
            )
        except Exception as exc:
            logger.warning(
                f"Failed to initialize MetadataCache for {data_source}: {exc}"
            )
            return None

    return st.session_state[cache_key]


def compute_filtered_row_count(
    filters: Dict[str, Any], data_source: Optional[str]
) -> Optional[int]:
    """Compute row count for current filters leveraging MetadataCache."""
    cache = get_metadata_cache(data_source)
    if cache is None:
        return None

    filters_to_apply = dict(filters or {})

    if filters_to_apply:
        filters_to_apply = normalize_filters_for_dataset(filters_to_apply, data_source)

    try:
        if filters_to_apply:
            metadata = cache.get_filtered_metadata(filters_to_apply)
        else:
            metadata = cache.get_global_metadata()

        shape = metadata.get("shape", {})
        return shape.get("rows")
    except Exception as exc:
        logger.warning(f"Failed to compute filtered row count: {exc}")
        return None


def sync_filter_state(
    session_manager: SessionStateManager, new_backend_filters: Dict[str, Any]
) -> None:
    """
    Sincroniza estado de filtros apos resposta do backend.

    Fluxo:
    1. Armazena filtros do backend
    2. Inicializa enabled state para novos filtros (default True)
    3. Remove filtros obsoletos (nao existem mais no backend)
    4. Calcula active_filters (apenas enabled=True)

    Args:
        session_manager: Gerenciador de sessao
        new_backend_filters: Filtros retornados em filter_final
    """
    # 1. Armazena filtros originais do backend
    session_manager.backend_filters = new_backend_filters

    # 2. Inicializa enabled state para novos filtros
    current_enabled = session_manager.user_enabled_filters.copy()
    for col in new_backend_filters.keys():
        if col not in current_enabled:
            current_enabled[col] = True  # Novos filtros default enabled

    # 3. Remove filtros que nao existem mais no backend
    current_enabled = {
        col: enabled
        for col, enabled in current_enabled.items()
        if col in new_backend_filters
    }
    session_manager.user_enabled_filters = current_enabled

    # 4. Calcula filtros ativos (apenas enabled)
    active_filters = {
        col: val
        for col, val in new_backend_filters.items()
        if current_enabled.get(col, True)
    }
    session_manager.active_filters_applied = active_filters

    logger.info(
        f"Sync - Backend: {len(new_backend_filters)}, Active: {len(active_filters)}"
    )


@st.cache_data(show_spinner=False)
def load_filter_sample_dataframe(
    data_source: Optional[str],
    columns: tuple[str, ...],
    sample_size: int = 2000,
    _session_id: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Load a lightweight dataframe with the specified columns for normalization."""
    if not data_source or not columns:
        return None

    cols_sql = ", ".join(f'"{col}"' for col in columns)
    query = f"SELECT {cols_sql} FROM '{data_source}' LIMIT {sample_size}"

    try:
        with duckdb.connect() as conn:
            return conn.execute(query).df()
    except Exception as exc:
        logger.warning(
            f"Failed to load sample dataframe for normalization ({columns}): {exc}"
        )
        return None


def _serialize_filter_value(value: Any) -> Any:
    """Convert filter values to JSON-serializable primitives."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize_filter_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_filter_value(item) for item in value]
    return value


def normalize_filters_for_dataset(
    filters: Dict[str, Any], data_source: Optional[str]
) -> Dict[str, Any]:
    """Normalize filters using dataset samples to fix casing and types."""
    columns = tuple(filters.keys())
    sample_df = load_filter_sample_dataframe(
        data_source,
        columns,
        _session_id=st.session_state.get("session_id", "default"),
    )

    if sample_df is None or sample_df.empty:
        return filters

    try:
        normalized = normalize_filters(sample_df, filters, case_sensitive=False)
        return {k: _serialize_filter_value(v) for k, v in normalized.items()}
    except Exception as exc:
        logger.warning(f"Failed to normalize filters for dataset: {exc}")
        return filters


def _render_header():
    """Renderiza cabecalho da aplicacao com design profissional"""
    # Import selected_model from config
    try:
        from src.shared_lib.core.config import SELECTED_MODEL as selected_model
    except (ImportError, AttributeError):
        selected_model = "gemini 2.5 flash"  # Fallback

    # Enhanced Professional Header - REFACTORED FOR PROPER CENTERING
    st.markdown(
        f"""
        <div class="header-container">
            <h1 class="app-title">AGENTE IA TARGET</h1>
            <p class="app-subtitle">INTELIGÊNCIA ARTIFICIAL PARA ANÁLISE DE DADOS</p>
            <div class="app-description">
                Converse naturalmente com seus dados comerciais.<br>
                Faça perguntas em linguagem natural e obtenha insights precisos através de análise inteligente.<br>
                <span style="font-size: 0.85rem; opacity: 0.99;">Modelo: {selected_model}</span>
            </div>
            <div class="feature-icons">
                <div class="feature-item">
                    <div class="feature-icon">💬</div>
                    <span>Chat Natural</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">📊</div>
                    <span>Análise Rápida</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">🎯</div>
                    <span>Insights Precisos</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">🚀</div>
                    <span>Resultados Instantâneos</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_footer():
    """Renderiza footer com logotipo da empresa"""
    # Footer Target Data Experience
    st.markdown("---")
    st.markdown("<br>", unsafe_allow_html=True)

    # Create footer with logo
    footer_col1, footer_col2, footer_col3 = st.columns([1, 2, 1])

    with footer_col2:
        # Company footer with modern styling
        st.markdown(
            """
            <div style="text-align: center; background: linear-gradient(135deg, #1a2332 0%, #2d3e50 100%);
                        padding: 30px; border-radius: 15px; margin: 20px 0; display: flex;
                        flex-direction: column; align-items: center; justify-content: center;">
                <div style="color: white; font-family: 'Arial', sans-serif; font-weight: 300;
                           letter-spacing: 6px; margin: 0; font-size: 24px;">T A R G E T</div>
                <div style="color: #e74c3c; font-family: 'Arial', sans-serif; font-weight: 300;
                          letter-spacing: 3px; margin: 8px 0 0 0; font-size: 12px;">D A T A &nbsp; E X P E R I E N C E</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)


def _get_base64_image(image_path: str) -> str:
    """Convert image to base64 for HTML embedding"""
    import base64

    with open(image_path, "rb") as image_file:
        encoded = base64.b64encode(image_file.read()).decode()
    return encoded


def initialize_app():
    """Initialize application state and components"""
    # Initialize session state manager
    if "session_manager" not in st.session_state:
        st.session_state.session_manager = SessionStateManager(st.session_state)

    # Initialize pipeline runner
    if "pipeline_runner" not in st.session_state:
        st.session_state.pipeline_runner = StreamingPipelineRunner()

    return st.session_state.session_manager


def render_sidebar(session_manager: SessionStateManager):
    """
    Render sidebar with logo, controls, and filter display

    Args:
        session_manager: Session state manager instance
    """
    with st.sidebar:
        # Logo - Fixed at top with reduced size
        logo_path = Path("streamlit_app/images/target_solucoes_comerciais_logo.png")
        if logo_path.exists():
            st.markdown(
                f"""
                <div class="sidebar-logo-container">
                    <img src="data:image/png;base64,{_get_base64_image(str(logo_path))}" />
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.title("📊 Analytics Chatbot")

        # Dataset Information (if available)
        try:
            # Try to get dataset info from pipeline runner
            pipeline_runner = st.session_state.get("pipeline_runner")
            if (
                pipeline_runner
                and hasattr(pipeline_runner, "df")
                and pipeline_runner.df is not None
            ):
                df = pipeline_runner.df
                st.markdown("---")
                st.markdown("## 📊 Informacoes do Dataset")
                st.markdown(f"**Registros totais:** {len(df):,}")

                # Try to get date range if 'Data' column exists
                if "Data" in df.columns:
                    try:
                        min_date = df["Data"].min()
                        max_date = df["Data"].max()
                        st.markdown(
                            f"**Periodo:** {min_date.strftime('%Y-%m-%d')} a "
                            f"{max_date.strftime('%Y-%m-%d')}"
                        )
                    except Exception:
                        pass
        except Exception:
            # If dataset info is not available, skip this section silently
            pass

        st.markdown("---")

        # Developer Mode indicator
        if DEVELOPER_MODE:
            st.sidebar.caption("🛠️ Developer Mode ativo")
            st.markdown("---")

        # Clear conversation button - CINZA CLARO
        st.markdown(
            "<style>"
            "/* Botão Limpar Conversa - MÁXIMA PRIORIDADE CINZA */"
            'section[data-testid="stSidebar"] button[key="sidebar_clear_conversation"],'
            'section[data-testid="stSidebar"] div[data-testid="stButton"] button[key="sidebar_clear_conversation"],'
            '.stSidebar button[key="sidebar_clear_conversation"],'
            'div[data-testid="stButton"] button[key="sidebar_clear_conversation"] {'
            "    background: linear-gradient(135deg, #95a5a6 0%, #7f8c8d 100%) !important;"
            "    color: white !important;"
            "    border: none !important;"
            "    font-weight: 600 !important;"
            "    box-shadow: 0 2px 8px rgba(149, 165, 166, 0.3) !important;"
            "}"
            'section[data-testid="stSidebar"] button[key="sidebar_clear_conversation"]:hover,'
            'section[data-testid="stSidebar"] div[data-testid="stButton"] button[key="sidebar_clear_conversation"]:hover,'
            '.stSidebar button[key="sidebar_clear_conversation"]:hover,'
            'div[data-testid="stButton"] button[key="sidebar_clear_conversation"]:hover {'
            "    background: linear-gradient(135deg, #7f8c8d 0%, #6c757d 100%) !important;"
            "    transform: translateY(-1px) !important;"
            "    box-shadow: 0 3px 12px rgba(127, 140, 141, 0.4) !important;"
            "}"
            "</style>",
            unsafe_allow_html=True,
        )
        if st.button(
            "🗑️ Limpar Conversa",
            help="Limpar toda a conversa e filtros",
            use_container_width=True,
            key="sidebar_clear_conversation",
            type="secondary",
        ):
            session_manager.reset_conversation()
            st.rerun()

        st.markdown("---")

        # About section
        with st.expander("💡 Sobre"):
            st.markdown("""
            **Analytics Chatbot v1.0**

            Sistema de analise interativa que processa consultas em linguagem natural
            e gera visualizacoes e insights automaticamente.

            **Pipeline Multi-Agente:**
            - Filter Classifier
            - Graphic Classifier
            - Analytics Executor
            - Plotly Generator
            - Insight Generator
            - Formatter Agent
            """)

        # ========== ALIAS MANAGER SECTION ==========
        st.markdown("---")

        # Botão para abrir o gerenciador de aliases
        if st.button(
            "🔤 Gerenciar Aliases",
            help="Visualizar e editar aliases semânticos de colunas",
            use_container_width=True,
            key="open_alias_manager",
        ):
            st.session_state.show_alias_manager = not st.session_state.get(
                "show_alias_manager", False
            )

        # =========================================

        # ========== ACTIVE FILTERS SECTION ==========
        st.markdown("---")
        st.markdown("## 🔍 Filtros Ativos")

        # Get filter data from session manager
        try:
            backend_filters = session_manager.backend_filters
            user_enabled = session_manager.user_enabled_filters

            if backend_filters and len(backend_filters) > 0:
                # Botao global: Limpar Todos
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("**Gerenciar Filtros:**")
                with col2:
                    if st.button("Limpar", key="clear_all_filters", type="secondary"):
                        session_manager.user_enabled_filters = {
                            col: False for col in backend_filters.keys()
                        }
                        session_manager.active_filters_applied = {}
                        st.rerun()

                st.markdown("---")

                # Renderiza cada filtro com checkbox
                for column, value in backend_filters.items():
                    is_enabled = user_enabled.get(column, True)

                    filter_col1, filter_col2 = st.columns([0.1, 0.9])

                    with filter_col1:
                        # Checkbox individual
                        enabled = st.checkbox(
                            "",
                            value=is_enabled,
                            key=f"filter_checkbox_{column}",
                            label_visibility="collapsed",
                        )

                        # Detecta mudanca de estado
                        if enabled != is_enabled:
                            updated_enabled = user_enabled.copy()
                            updated_enabled[column] = enabled
                            session_manager.user_enabled_filters = updated_enabled

                            # Recalcula active_filters
                            active_filters = {
                                col: val
                                for col, val in backend_filters.items()
                                if updated_enabled.get(col, True)
                            }
                            session_manager.active_filters_applied = active_filters
                            st.rerun()

                    with filter_col2:
                        # Formata valor
                        if isinstance(value, list):
                            value_str = ", ".join(str(v) for v in value)
                        else:
                            value_str = str(value)

                        # Estilo condicional
                        if enabled:
                            st.markdown(f"**{column}:** {value_str}")
                        else:
                            st.markdown(
                                f'<span style="color: gray; text-decoration: line-through;">'
                                f"**{column}:** {value_str}</span>",
                                unsafe_allow_html=True,
                            )

                # Contador de filtros ativos
                active_count = sum(1 for v in user_enabled.values() if v)
                total_count = len(backend_filters)
                st.info(f"{active_count} de {total_count} filtros ativos")

            else:
                st.markdown("*Nenhum filtro aplicado*")

            # Contagem de registros
            row_count = session_manager.active_row_count
            if row_count is not None:
                st.markdown(f"**📊 Registros:** {row_count:,}")

        except Exception as e:
            st.error(f"Erro ao exibir filtros: {e}")
            import traceback

            st.code(traceback.format_exc())


def process_user_query(query: str, session_manager: SessionStateManager):
    """
    Process user query with progressive display

    Supports both formatter_output (graphical) and non_graph_output (textual) responses.
    The pipeline automatically routes to the appropriate agent based on query type.

    Args:
        query: User query string
        session_manager: Session state manager
    """
    # Add user message to history
    session_manager.chat_history.add_user_message(query)

    # Create message container for assistant response
    with st.chat_message("assistant"):
        # Initialize progressive renderer
        renderer = ProgressiveRenderer()

        # Create streaming display manager (will show initial spinner)
        display_manager = StreamingDisplayManager(renderer)

        # Get pipeline runner
        pipeline_runner = st.session_state.pipeline_runner

        try:
            # Execute pipeline with streaming
            final_state = None
            for state_update in pipeline_runner.run_with_streaming(
                query=query,
                reset_filters=False,
                current_filters=session_manager.active_filters_applied,
                filter_history=session_manager.filter_history,
            ):
                # Process each state update for progressive display
                display_manager.process_pipeline_state(state_update)
                final_state = state_update

            # Detect output type and extract data
            from src.shared_lib.utils.output_detector import detect_output_type

            if final_state:
                try:
                    output_type, output_data = detect_output_type(final_state)

                    # Get execution time
                    from src.shared_lib.utils.output_detector import get_execution_time

                    execution_time = get_execution_time(output_data)

                    # Add to chat history
                    session_manager.chat_history.add_assistant_message(
                        output_data, execution_time
                    )

                    # ========== UPDATE ACTIVE FILTERS AND ROW COUNT ==========
                    try:
                        # Extrai filtros do backend (filter_final)
                        new_backend_filters = final_state.get("filter_final", {}) or {}
                        data_source = final_state.get("data_source")

                        # Sincroniza estado (backend → user enabled → active)
                        sync_filter_state(session_manager, new_backend_filters)

                        # Compute row count para filtros ATIVOS
                        active_filters = session_manager.active_filters_applied
                        row_count = compute_filtered_row_count(
                            active_filters, data_source
                        )

                        # Fallback to metadata
                        if row_count is None:
                            metadata = output_data.get("metadata", {})
                            row_count = metadata.get("filtered_dataset_row_count")
                            if row_count is None:
                                row_count = metadata.get("row_count")

                        session_manager.active_row_count = row_count
                        session_manager.filter_history = (
                            final_state.get("filter_history", []) or []
                        )

                        logger.info(f"Backend filters: {new_backend_filters}")
                        logger.info(f"Active filters: {active_filters}")
                        logger.info(f"Filtered row count: {row_count}")

                    except Exception as filter_error:
                        logger.error(
                            f"Failed to update filters: {filter_error}", exc_info=True
                        )
                        import traceback

                        logger.error(traceback.format_exc())
                    # =========================================================

                    # ========== SESSION LOGGING (SUCESSO) ==========
                    try:
                        from src.shared_lib.utils.query_data_extractor import (
                            extract_query_data,
                        )

                        query_id = len(session_manager.chat_history.messages) // 2

                        query_data = extract_query_data(
                            query=query,
                            output_data=output_data,
                            query_id=query_id,
                            output_type=output_type,
                        )

                        session_logger = st.session_state.get("session_logger")
                        if session_logger:
                            session_logger.log_query(
                                query_data,
                                output_data=output_data,
                                output_type=output_type,
                            )

                    except Exception as log_error:
                        logger.warning(f"Failed to log query: {log_error}")
                    # ===============================================

                except ValueError as e:
                    # No output found
                    renderer.render_error(f"Erro: {str(e)}")

                    # Add error to history
                    error_output = {"status": "error", "error": str(e)}
                    session_manager.chat_history.add_assistant_message(
                        error_output, 0.0
                    )

        except Exception as e:
            renderer.render_error(f"Erro ao processar consulta: {str(e)}")

            # Add error to history
            error_output = {"status": "error", "error": str(e)}
            session_manager.chat_history.add_assistant_message(error_output, 0.0)

            # ========== SESSION LOGGING (ERRO) ==========
            try:
                from datetime import datetime

                query_id = len(session_manager.chat_history.messages) // 2

                error_query_data = {
                    "query_id": query_id,
                    "timestamp": datetime.now().isoformat(),
                    "user_query": query,
                    "output_type": "error",
                    "status": "error",
                    "execution_time": 0.0,
                    "error": str(e),
                    "formatter_output_reference": None,
                    "non_graph_output_reference": None,
                }

                session_logger = st.session_state.get("session_logger")
                if session_logger:
                    session_logger.log_query(error_query_data, output_data=None)

            except Exception as log_error:
                logger.warning(f"Failed to log error: {log_error}")
            # ===========================================


def render_chat_history(session_manager: SessionStateManager):
    """
    Render chat history from session

    Supports both formatter_output and non_graph_output responses.

    Args:
        session_manager: Session state manager
    """
    from streamlit_app.display_components import render_unified_response
    from src.shared_lib.utils.output_detector import detect_output_type

    for message in session_manager.chat_history.messages:
        if message.role == "user":
            with st.chat_message("user"):
                st.markdown(message.content)
        else:
            with st.chat_message("assistant"):
                try:
                    import json

                    output_data = json.loads(message.content)

                    # Detect output type and render accordingly
                    try:
                        output_type, _ = detect_output_type(
                            {"formatter_output": output_data}
                            if "executive_summary" in output_data
                            else {"non_graph_output": output_data}
                        )
                        render_unified_response(output_type, output_data)
                    except ValueError:
                        # Fallback: try to determine from content structure
                        if "executive_summary" in output_data:
                            render_unified_response("formatter", output_data)
                        elif "summary" in output_data:
                            render_unified_response("non_graph", output_data)
                        else:
                            render_complete_response(output_data)

                except json.JSONDecodeError:
                    st.error("Erro ao renderizar resposta")


def _ensure_client_session_monitor(
    timeout_manager: SessionTimeoutManager,
) -> None:
    """Render the client-side session monitor and handle emitted events."""
    if "session_monitor" not in st.session_state:
        monitor_config = SessionMonitorConfig(
            inactivity_timeout_minutes=SESSION_TIMEOUT_MINUTES,
            heartbeat_interval_seconds=30,
            warning_lead_minutes=min(
                SessionTimeoutManager.WARNING_BEFORE_TIMEOUT_MINUTES,
                max(SESSION_TIMEOUT_MINUTES * 0.5, 0.5),
            ),
        )
        st.session_state["session_monitor"] = SessionMonitorComponent(
            config=monitor_config
        )

    session_monitor: SessionMonitorComponent = st.session_state["session_monitor"]
    monitor_event = session_monitor.render()
    if not monitor_event:
        return

    action = monitor_event.get("action")
    if action == "update_heartbeat":
        # IMPORTANTE: Heartbeat NAO atualiza last_activity (inatividade)
        # Apenas atualiza last_heartbeat para detecção de aba fechada (15 min)
        # Inatividade é atualizada APENAS por ações do usuário (queries, cliques)
        session_id = st.session_state.get("session_id")
        if session_id:
            record_client_heartbeat(session_id)
        logger.debug("Heartbeat recebido do cliente - monitoramento de conexao")
        return

    if action == "close_session":
        reason = monitor_event.get("reason", "page_close")
        logger.info("Fechamento de sessao solicitado via cliente (%s)", reason)
        EmailAuthComponent.logout()
        st.stop()

    if action == "timeout_session":
        inactive_minutes = monitor_event.get("inactive_minutes")
        logger.info(
            "Timeout detectado no cliente apos %s minutos. Encerrando sessao.",
            inactive_minutes,
        )
        timeout_manager.update_activity(monitor_event.get("last_activity_ts"))
        timeout_manager.handle_timeout()
        st.stop()

    if action == "timeout_warning":
        logger.info("Aviso de inatividade emitido pelo cliente")
        timeout_manager.update_activity(monitor_event.get("last_activity_ts"))
        timeout_manager.render_warning(
            minutes_left=monitor_event.get("minutes_until_timeout")
        )
        return

    # Page visibility events are informative only for now
    if action == "page_hidden":
        logger.debug("Pagina em background - aguardando atividade")
    elif action == "page_visible":
        logger.debug("Pagina voltou a ficar visivel")


def main():
    """Main application entry point"""
    # Validate dataset configuration on first run
    try:
        _validate_dataset_on_startup()
    except DatasetIntegrityError as e:
        st.error(
            "Erro de configuracao do dataset. "
            "Verifique alias.yaml e DATASET_PATH.\n\n"
            f"Detalhes: {e}"
        )
        st.stop()

    # Initialize authentication
    EmailAuthComponent.initialize_session()

    # Require authentication before proceeding
    if not EmailAuthComponent.require_authentication():
        # User is not authenticated, auth screen is shown
        # Stop execution here
        return

    # User is authenticated, proceed with normal app flow

    if "timeout_manager" not in st.session_state:
        st.session_state["timeout_manager"] = SessionTimeoutManager(
            timeout_minutes=SESSION_TIMEOUT_MINUTES
        )

    timeout_manager: SessionTimeoutManager = st.session_state["timeout_manager"]

    if timeout_manager.check_and_handle_timeout():
        st.error(
            "Sessao encerrada por inatividade. Faca login novamente para continuar."
        )
        st.stop()

    # ========== CHECK IF SESSION WAS CLOSED BY BACKGROUND PROCESS ==========
    # Check after timeout_manager to catch sessions closed by SessionActivityTracker
    session_logger_check = st.session_state.get("session_logger")
    if session_logger_check and not session_logger_check.is_session_active():
        st.warning(
            "**Sessao Encerrada**\n\n"
            f"Sua sessao foi encerrada automaticamente.\n\n"
            f"Status: `{session_logger_check.get_session_status()}`\n\n"
            "Por favor, faca login novamente para continuar."
        )
        if st.button("Fazer Login Novamente", type="primary"):
            EmailAuthComponent.logout()
            st.rerun()
        st.stop()
    # ========================================================================

    # ========== INITIALIZE SESSION ID ==========
    if "session_id" not in st.session_state:
        from uuid import uuid4
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state["session_id"] = f"{uuid4().hex[:8]}_{timestamp}"
    # ===========================================
    session_id = st.session_state["session_id"]

    # Initialize app
    session_manager = initialize_app()

    # ========== INITIALIZE SESSION LOGGER ==========
    if "session_logger_initialized" not in st.session_state:
        from src.shared_lib.utils.session_logger import SessionLogger

        user_email = EmailAuthComponent.get_authenticated_email()
        session_id = st.session_state["session_id"]

        session_logger = SessionLogger(session_id, user_email)
        session_logger.create_session_file()

        st.session_state["session_logger"] = session_logger
        st.session_state["session_logger_initialized"] = True

    # ===============================================
    session_logger = st.session_state.get("session_logger")
    if session_logger:
        register_session_activity(session_id, session_logger)

    _ensure_client_session_monitor(timeout_manager)

    # Apply custom styling
    _setup_page_styling()

    # Render sidebar
    render_sidebar(session_manager)

    # Render authentication status in sidebar
    EmailAuthComponent.render_auth_header()

    # Render header
    _render_header()

    # Main chat interface with centered layout
    main_col1, main_col2, main_col3 = st.columns([0.5, 4, 0.5])

    with main_col2:
        # ========== ALIAS MANAGER VIEW ==========
        # Se o usuário ativou o gerenciador de aliases, mostra a interface completa
        if st.session_state.get("show_alias_manager", False):
            # Botão para voltar ao chat
            if st.button("⬅️ Voltar ao Chat", key="back_to_chat"):
                st.session_state.show_alias_manager = False
                st.rerun()

            st.markdown("<br>", unsafe_allow_html=True)

            # Renderiza interface do gerenciador
            try:
                alias_manager = get_alias_manager()
                alias_manager.render()
            except Exception as e:
                st.error(f"Erro ao carregar gerenciador de aliases: {e}")
                import traceback

                with st.expander("🔍 Detalhes do Erro"):
                    st.code(traceback.format_exc())

                if st.button("🔄 Tentar Novamente"):
                    st.rerun()

            # Para a execução aqui - não mostra o chat
            return

        # ========== CHAT VIEW (DEFAULT) ==========
        # Display chat history
        render_chat_history(session_manager)

        # ========== VALIDATE SESSION IS ACTIVE BEFORE ALLOWING INPUT ==========
        session_logger = st.session_state.get("session_logger")
        if session_logger and not session_logger.is_session_active():
            st.error(
                "Esta sessao foi encerrada. Por favor, faca login novamente para continuar."
            )
            if st.button("Fazer Login Novamente"):
                EmailAuthComponent.logout()
                st.rerun()
            st.stop()
        # ========================================================================

        # Chat input (only if session is active)
        if prompt := st.chat_input(
            "Digite sua pergunta...", disabled=session_manager.is_processing
        ):
            timeout_manager.update_activity()

            # Double-check session status (race condition protection)
            if session_logger and not session_logger.is_session_active():
                st.error(
                    "Sessao encerrada durante processamento. Faca login novamente."
                )
                st.stop()

            # Display user message immediately
            with st.chat_message("user"):
                st.markdown(prompt)

            # Process query
            session_manager.is_processing = True
            try:
                process_user_query(prompt, session_manager)

                # ALWAYS force rerun to update sidebar after query processing
                # This ensures the sidebar reflects the latest state
                logger.info("🔄 Query processed - Forcing rerun to update sidebar")
                st.rerun()
            finally:
                session_manager.is_processing = False

    # Developer Mode Panel rendering
    if DEVELOPER_MODE:
        from streamlit_app.developer_mode import render_developer_panel
        render_developer_panel(
            result=None,
            state_snapshots=st.session_state.get("dev_snapshots", []),
            performance_monitor=None
        )


if __name__ == "__main__":
    main()
