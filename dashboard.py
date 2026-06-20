# -*- coding: utf-8 -*-
from __future__ import annotations

from tkinter import messagebox

from dashboard_app import DashboardApp
from path_helpers import ensure_environment


def main() -> int:
    environment = ensure_environment()
    app = DashboardApp()

    def _show_environment_notice() -> None:
        notices: list[str] = []
        if not environment.get("ffmpeg"):
            notices.append("未检测到 ffmpeg，渲染功能可能不可用。请先安装 ffmpeg 并加入 PATH。")
        if not environment.get("browser_api"):
            notices.append("当前未检测到 BitBrowser / HubStudio 本地 API。浏览器启动后可再刷新分组。")
        if notices:
            messagebox.showwarning("环境检查提示", "\n".join(notices), parent=app)

    app.after(250, _show_environment_notice)
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
