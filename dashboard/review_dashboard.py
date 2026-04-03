from __future__ import annotations
import tkinter as tk
from tkinter import ttk
from models.shared_models import ReviewPacket
from dashboard.dashboard_actions import confirm_review

class ReviewDashboard(tk.Tk):
    def __init__(self, packet: ReviewPacket):
        super().__init__()
        self.packet = packet
        self.title("Scanner MVP Review Dashboard")
        self.geometry("900x650")
        self._build_ui()

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=12)
        top.pack(fill="both", expand=True)

        ttk.Label(top, text="Document Review", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        ttk.Label(top, text=f"File: {self.packet.scan_job.filename}").pack(anchor="w", pady=(2, 10))

        summary = tk.Text(top, height=4, wrap="word")
        summary.insert("1.0", self.packet.extraction.summary or "No summary available.")
        summary.configure(state="disabled")
        summary.pack(fill="x", pady=(0, 10))

        form = ttk.Frame(top)
        form.pack(fill="x", pady=6)

        self.fields = {}
        rows = [
            ("document_type", self.packet.extraction.document_type or ""),
            ("vendor", self.packet.extraction.vendor or ""),
            ("amount", "" if self.packet.extraction.amount is None else str(self.packet.extraction.amount)),
            ("document_date", self.packet.extraction.document_date or ""),
            ("account_last4", self.packet.extraction.account_last4 or ""),
            ("property_hint", self.packet.extraction.property_hint or ""),
        ]
        for i, (label, value) in enumerate(rows):
            ttk.Label(form, text=label).grid(row=i, column=0, sticky="w", padx=(0,10), pady=4)
            entry = ttk.Entry(form, width=50)
            entry.insert(0, value)
            entry.grid(row=i, column=1, sticky="ew", pady=4)
            self.fields[label] = entry
        form.columnconfigure(1, weight=1)

        warnings = "\n".join(self.packet.extraction.warnings) if self.packet.extraction.warnings else "None"
        ttk.Label(top, text=f"Warnings: {warnings}").pack(anchor="w", pady=(10, 0))
        ttk.Label(top, text=f"Proposed filename: {self.packet.proposed_filename or 'TBD'}").pack(anchor="w")
        ttk.Label(top, text=f"Proposed property: {self.packet.proposed_property or 'Unassigned'}").pack(anchor="w")

        btns = ttk.Frame(top)
        btns.pack(fill="x", pady=18)
        ttk.Button(btns, text="Confirm", command=self._confirm).pack(side="left")
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="left", padx=8)

        ttk.Label(top, text="Recent scans").pack(anchor="w", pady=(18, 6))
        recent = tk.Text(top, height=8, wrap="word")
        lines = []
        for item in self.packet.recent_scan_items[:10]:
            lines.append(f"- {item.get('filename')} | {item.get('status')} | {item.get('summary','')}")
        recent.insert("1.0", "\n".join(lines) if lines else "No recent scans.")
        recent.configure(state="disabled")
        recent.pack(fill="both", expand=True)

    def _confirm(self) -> None:
        edits = {k: v.get().strip() for k, v in self.fields.items()}
        if edits.get("amount") == "":
            edits["amount"] = None
        else:
            try:
                edits["amount"] = float(edits["amount"])
            except ValueError:
                pass
        confirm_review(self.packet, edits)
        self.destroy()
