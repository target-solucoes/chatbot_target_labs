"""
File Saver Utility for Plotly Charts

This module provides utilities for saving Plotly charts to disk in various formats:
- HTML (interactive, default)
- PNG (static, requires kaleido)

Features:
- Automatic filename generation
- Directory management
- Error handling for missing dependencies
- Configurable Plotly.js inclusion modes

Author: Claude Code
Date: 2025-11-12
Version: 1.0
"""

from pathlib import Path
from datetime import datetime
from typing import Optional
import plotly.graph_objects as go

from src.shared_lib.utils.logger import get_logger

logger = get_logger(__name__)


class FileSaver:
    """
    Manager for saving Plotly charts to disk.

    Supports multiple output formats:
    - HTML: Interactive charts with Plotly.js
    - PNG: Static images (requires kaleido package)

    Example Usage:
        >>> saver = FileSaver(output_dir=Path("charts"))
        >>> fig = go.Figure(data=[go.Bar(x=[1,2,3], y=[4,5,6])])
        >>> html_path = saver.save_html(fig)
        >>> png_path = saver.save_png(fig, width=1200, height=800)
    """

    def __init__(self, output_dir: Path):
        """
        Initialize FileSaver.

        Args:
            output_dir: Directory where charts will be saved
                        Will be created if it doesn't exist
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"FileSaver initialized with output_dir: {self.output_dir}")

    def save_html(
        self,
        fig: go.Figure,
        filename: Optional[str] = None,
        include_plotlyjs: str = "cdn",
    ) -> Path:
        """
        Save chart as interactive HTML file.

        Args:
            fig: Plotly Figure object to save
            filename: Custom filename (optional, auto-generated if None)
                      Example: "sales_chart.html"
            include_plotlyjs: How to include Plotly.js library
                - "cdn": Load from CDN (smaller file, requires internet)
                - "inline": Embed full library (larger file, works offline)
                - False: Don't include (for embedding in existing page)

        Returns:
            Path: Full path to saved HTML file

        Raises:
            IOError: If file cannot be written

        Example:
            >>> saver = FileSaver(Path("output"))
            >>> fig = create_my_chart()
            >>> path = saver.save_html(fig, filename="report.html")
            >>> print(f"Saved to: {path}")
        """
        if filename is None:
            filename = self._generate_filename(fig, "html")

        filepath = self.output_dir / filename

        try:
            fig.write_html(
                str(filepath),
                include_plotlyjs=include_plotlyjs,
                config={
                    "displayModeBar": True,
                    "displaylogo": False,
                    "modeBarButtonsToRemove": ["sendDataToCloud"],
                },
            )
            logger.info(
                f"Chart saved as HTML: {filepath} ({self._get_file_size(filepath)})"
            )
            return filepath

        except Exception as e:
            logger.error(f"Failed to save HTML file: {e}", exc_info=True)
            raise IOError(f"Could not save HTML to {filepath}: {e}")

    def save_png(
        self,
        fig: go.Figure,
        filename: Optional[str] = None,
        width: int = 1200,
        height: int = 800,
        scale: float = 2.0,
    ) -> Path:
        """
        Save chart as static PNG image.

        IMPORTANT: Requires 'kaleido' package to be installed:
            pip install kaleido

        Args:
            fig: Plotly Figure object to save
            filename: Custom filename (optional, auto-generated if None)
            width: Image width in pixels (default: 1200)
            height: Image height in pixels (default: 800)
            scale: Scaling factor for resolution (default: 2.0 for retina)
                   Higher values = better quality but larger files

        Returns:
            Path: Full path to saved PNG file

        Raises:
            ImportError: If kaleido is not installed
            IOError: If file cannot be written

        Example:
            >>> saver = FileSaver(Path("output"))
            >>> fig = create_my_chart()
            >>> path = saver.save_png(fig, width=1920, height=1080)
            >>> print(f"Saved high-res PNG to: {path}")
        """
        if filename is None:
            filename = self._generate_filename(fig, "png")

        filepath = self.output_dir / filename

        try:
            fig.write_image(str(filepath), width=width, height=height, scale=scale)
            logger.info(
                f"Chart saved as PNG: {filepath} "
                f"({width}x{height}px, scale={scale}, {self._get_file_size(filepath)})"
            )
            return filepath

        except ImportError:
            error_msg = (
                "Kaleido package not installed. To save PNG images, install it:\n"
                "  pip install kaleido\n"
                "or\n"
                "  poetry add kaleido"
            )
            logger.error(error_msg)
            raise ImportError(error_msg)

        except Exception as e:
            logger.error(f"Failed to save PNG file: {e}", exc_info=True)
            raise IOError(f"Could not save PNG to {filepath}: {e}")

    def _generate_filename(self, fig: go.Figure, extension: str) -> str:
        """
        Generate unique filename based on timestamp and chart type.

        Format: chart_{chart_type}_{timestamp}.{extension}
        Example: chart_bar_horizontal_20251112_143022.html

        Args:
            fig: Plotly Figure (used to extract chart type if available)
            extension: File extension (html, png, etc.)

        Returns:
            str: Generated filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Try to extract chart type from figure layout title or data
        chart_type = "chart"
        try:
            if fig.data and hasattr(fig.data[0], "type"):
                chart_type = fig.data[0].type
        except Exception:
            pass  # Use default "chart" if extraction fails

        filename = f"chart_{chart_type}_{timestamp}.{extension}"
        logger.debug(f"Generated filename: {filename}")
        return filename

    def _get_file_size(self, filepath: Path) -> str:
        """
        Get human-readable file size.

        Args:
            filepath: Path to file

        Returns:
            str: Formatted file size (e.g., "1.2 MB", "345 KB")
        """
        try:
            size_bytes = filepath.stat().st_size

            if size_bytes < 1024:
                return f"{size_bytes} bytes"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        except Exception:
            return "unknown size"

    def list_saved_files(self, extension: Optional[str] = None) -> list[Path]:
        """
        List all saved chart files in output directory.

        Args:
            extension: Filter by extension (e.g., "html", "png")
                       If None, returns all files

        Returns:
            List of Path objects for saved files

        Example:
            >>> saver = FileSaver(Path("output"))
            >>> html_files = saver.list_saved_files("html")
            >>> print(f"Found {len(html_files)} HTML charts")
        """
        if extension:
            pattern = f"*.{extension}"
        else:
            pattern = "*.*"

        files = sorted(self.output_dir.glob(pattern))
        logger.debug(f"Found {len(files)} files matching {pattern}")
        return files

    def clear_old_files(self, days: int = 7, extension: Optional[str] = None) -> int:
        """
        Delete files older than specified number of days.

        Useful for cleanup of temporary or demo charts.

        Args:
            days: Delete files older than this many days
            extension: Only delete files with this extension (e.g., "html")
                       If None, affects all files

        Returns:
            int: Number of files deleted

        Example:
            >>> saver = FileSaver(Path("output"))
            >>> deleted = saver.clear_old_files(days=30, extension="html")
            >>> print(f"Deleted {deleted} old HTML files")
        """
        import time as time_module

        cutoff_time = time_module.time() - (days * 24 * 60 * 60)
        deleted_count = 0

        for file_path in self.list_saved_files(extension):
            try:
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
                    logger.debug(f"Deleted old file: {file_path}")
            except Exception as e:
                logger.warning(f"Could not delete {file_path}: {e}")

        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} files older than {days} days")

        return deleted_count
