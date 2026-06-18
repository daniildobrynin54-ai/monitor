#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MangaBuff Alliance Monitor — v2.0 (стабильная версия)
"""

from monitor import MangaBuffMonitor


def main():
    print("""
╔══════════════════════════════════════════════════════╗
║   MangaBuff Alliance Monitor  v2.0                   ║
║   Мониторинг смены тайтла в альянсе                  ║
╠══════════════════════════════════════════════════════╣
    """)

    monitor = MangaBuffMonitor()
    monitor.start()


if __name__ == "__main__":
    main()