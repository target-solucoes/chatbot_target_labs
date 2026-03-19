# -*- coding: utf-8 -*-
"""
Session State Management for Streamlit Chatbot

Manages chat history, filter state synchronization, and session persistence.
"""

import json
from src.shared_lib.utils.json_serialization import json_dumps
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Optional, Any


@dataclass
class ChatMessage:
    """Represents a single chat message"""

    role: str  # 'user' or 'assistant'
    content: str  # User query or assistant response (JSON for assistant)
    timestamp: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class ChatHistory:
    """Manages chat message history"""

    messages: List[ChatMessage] = field(default_factory=list)

    def add_user_message(self, query: str) -> None:
        """Add user query to history"""
        message = ChatMessage(
            role="user", content=query, timestamp=datetime.now().isoformat()
        )
        self.messages.append(message)

    def add_assistant_message(
        self, formatter_output: Dict, execution_time: float = 0.0
    ) -> None:
        """Add assistant response to history"""
        message = ChatMessage(
            role="assistant",
            content=json_dumps(formatter_output, ensure_ascii=False),
            timestamp=datetime.now().isoformat(),
            metadata={
                "execution_time": execution_time,
                "status": formatter_output.get("status", "unknown"),
            },
        )
        self.messages.append(message)

    def get_last_assistant_response(self) -> Optional[Dict]:
        """Get the last assistant response as parsed JSON"""
        for message in reversed(self.messages):
            if message.role == "assistant":
                try:
                    return json.loads(message.content)
                except json.JSONDecodeError:
                    return None
        return None

    def clear(self) -> None:
        """Clear all messages"""
        self.messages.clear()

    def to_list(self) -> List[Dict]:
        """Convert to list of dicts for serialization"""
        return [msg.to_dict() for msg in self.messages]

    def __len__(self) -> int:
        return len(self.messages)


class SessionStateManager:
    """
    Centralized session state manager for Streamlit app

    Manages chat history, filter state, and UI state
    """

    def __init__(self, session_state):
        """
        Initialize with Streamlit session_state

        Args:
            session_state: st.session_state object
        """
        self.state = session_state
        self._initialize_state()

    def _initialize_state(self) -> None:
        """Initialize default session state values"""
        if "chat_history" not in self.state:
            self.state.chat_history = ChatHistory()
            # Add welcome message automatically on first initialization
            self._add_welcome_message()

        if "processing" not in self.state:
            self.state.processing = False

        if "current_response" not in self.state:
            self.state.current_response = None

        if "show_debug" not in self.state:
            self.state.show_debug = False

        if "active_filters_applied" not in self.state:
            self.state.active_filters_applied = {}

        if "active_row_count" not in self.state:
            self.state.active_row_count = None

        if "filter_history" not in self.state:
            self.state.filter_history = []

        if "backend_filters" not in self.state:
            self.state.backend_filters = {}

        if "user_enabled_filters" not in self.state:
            self.state.user_enabled_filters = {}

    def _add_welcome_message(self) -> None:
        """Add welcome message as first assistant message"""
        welcome_text = """Olá! Sou o **Agente IA Target**, seu assistente para análise de dados comerciais.

Estou aqui para ajudá-lo a explorar e entender seus dados através de conversas naturais. Você pode me fazer perguntas como:
- "Quais são os produtos mais vendidos?"
- "Quais estados apresentaram maior faturamento?"
- "Analise as tendências de vendas"

Como posso ajudar você hoje?"""

        # Create welcome message with proper formatter output structure
        welcome_output = {
            "status": "success",
            "executive_summary": {
                "title": "👋 Bem-vindo",
                "introduction": welcome_text,
            },
            "visualization": {},
            "insights": {},
            "next_steps": {},
            "data": {},
            "metadata": {"total_execution_time": 0.0},
        }

        self.state.chat_history.add_assistant_message(welcome_output, 0.0)

    @property
    def chat_history(self) -> ChatHistory:
        """Get chat history"""
        return self.state.chat_history

    @property
    def is_processing(self) -> bool:
        """Check if currently processing a query"""
        return self.state.processing

    @is_processing.setter
    def is_processing(self, value: bool) -> None:
        """Set processing state"""
        self.state.processing = value

    @property
    def current_response(self) -> Optional[Dict]:
        """Get current response being displayed"""
        return self.state.current_response

    @current_response.setter
    def current_response(self, value: Optional[Dict]) -> None:
        """Set current response"""
        self.state.current_response = value

    @property
    def show_debug(self) -> bool:
        """Check if debug panel should be shown"""
        return self.state.show_debug

    @show_debug.setter
    def show_debug(self, value: bool) -> None:
        """Set debug panel visibility"""
        self.state.show_debug = value

    @property
    def session_id(self) -> str:
        """Get current session ID"""
        return self.state.get("session_id", "unknown")

    @property
    def session_logger(self) -> "SessionLogger":
        """Get session logger instance"""
        if "session_logger" not in self.state:
            raise RuntimeError("SessionLogger not initialized")
        return self.state["session_logger"]

    @property
    def active_filters_applied(self) -> Dict[str, Any]:
        """Get currently active filters from last query"""
        return self.state.get("active_filters_applied", {})

    @active_filters_applied.setter
    def active_filters_applied(self, value: Dict[str, Any]) -> None:
        """Set active filters from query response"""
        self.state.active_filters_applied = value

    @property
    def active_row_count(self) -> Optional[int]:
        """Get row count from last query"""
        return self.state.get("active_row_count")

    @active_row_count.setter
    def active_row_count(self, value: Optional[int]) -> None:
        """Set row count from query response"""
        self.state.active_row_count = value

    @property
    def filter_history(self) -> List[Dict[str, Any]]:
        """Get history of filter operations for the session"""
        return self.state.get("filter_history", [])

    @filter_history.setter
    def filter_history(self, value: List[Dict[str, Any]]) -> None:
        """Persist filter history for conversational context"""
        self.state.filter_history = value

    @property
    def backend_filters(self) -> Dict[str, Any]:
        """Get filters returned by backend (filter_final)"""
        return self.state.get("backend_filters", {})

    @backend_filters.setter
    def backend_filters(self, value: Dict[str, Any]) -> None:
        """Set backend filters from pipeline response"""
        self.state.backend_filters = value

    @property
    def user_enabled_filters(self) -> Dict[str, bool]:
        """Get user-controlled filter enabled state (col -> True/False)"""
        return self.state.get("user_enabled_filters", {})

    @user_enabled_filters.setter
    def user_enabled_filters(self, value: Dict[str, bool]) -> None:
        """Set user filter enabled state"""
        self.state.user_enabled_filters = value

    def reset_conversation(self) -> None:
        """Reset entire conversation including filters"""
        self.chat_history.clear()
        self.current_response = None
        self.is_processing = False
        self.active_filters_applied = {}
        self.active_row_count = None
        self.filter_history = []
        self.backend_filters = {}
        self.user_enabled_filters = {}
        # Re-add welcome message after clearing
        self._add_welcome_message()
