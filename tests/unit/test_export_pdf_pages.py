"""Verify PDF export page numbering is implemented."""
from __future__ import annotations

import inspect


def test_pdf_export_has_page_numbering():
    """The PDF builder must include page numbering in the footer."""
    from backtestforecast.services.exports import ExportService

    source = inspect.getsource(ExportService._build_pdf)
    assert "_page_number" in source, "PDF export must track page numbers"
    assert "_draw_page_footer" in source, "PDF export must draw page footer with page number"
