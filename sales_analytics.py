# =============================================================================
# Sales Dashboard & SQL Analytics
# Tools: Python, SQLite, Pandas, Matplotlib, Seaborn
# =============================================================================

import warnings
warnings.filterwarnings("ignore")

import sqlite3
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
import seaborn as sns
from datetime import datetime, timedelta
import random

# ── Styling ────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.05)
PALETTE = {
    "blue":   "#2563EB",
    "green":  "#16A34A",
    "orange": "#EA580C",
    "purple": "#7C3AED",
    "red":    "#DC2626",
    "gray":   "#6B7280",
    "light":  "#F1F5F9",
    "dark":   "#1E293B",
}
DB_PATH  = "sales.db"
OUT_DIR  = "outputs"
os.makedirs(OUT_DIR, exist_ok=True)


# =============================================================================
# 1.  DATABASE SETUP — Schema + Synthetic Data
# =============================================================================

def setup_database():
    """Create SQLite schema and populate with realistic synthetic sales data."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # ── Schema ────────────────────────────────────────────────────────────────
    cur.executescript("""
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS regions;

        CREATE TABLE regions (
            region_id   INTEGER PRIMARY KEY,
            region_name TEXT NOT NULL
        );

        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email         TEXT,
            region_id     INTEGER REFERENCES regions(region_id),
            segment       TEXT        -- Consumer / Corporate / Home Office
        );

        CREATE TABLE products (
            product_id    INTEGER PRIMARY KEY,
            product_name  TEXT NOT NULL,
            category      TEXT NOT NULL,   -- Technology / Furniture / Office Supplies
            sub_category  TEXT NOT NULL,
            unit_price    REAL NOT NULL
        );

        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(customer_id),
            order_date  TEXT NOT NULL,
            ship_date   TEXT NOT NULL,
            status      TEXT DEFAULT 'Completed'
        );

        CREATE TABLE order_items (
            item_id     INTEGER PRIMARY KEY,
            order_id    INTEGER REFERENCES orders(order_id),
            product_id  INTEGER REFERENCES products(product_id),
            quantity    INTEGER NOT NULL,
            discount    REAL    DEFAULT 0.0,
            revenue     REAL    NOT NULL
        );
    """)

    # ── Seed data ─────────────────────────────────────────────────────────────
    random.seed(42)
    np.random.seed(42)

    regions = [(1,"North"),(2,"South"),(3,"East"),(4,"West"),(5,"Central")]
    cur.executemany("INSERT INTO regions VALUES (?,?)", regions)

    segments = ["Consumer","Corporate","Home Office"]
    customers = [
        (i, f"Customer_{i:04d}", f"cust{i}@email.com",
         random.randint(1,5), random.choice(segments))
        for i in range(1, 501)
    ]
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", customers)

    products_data = [
        # Technology
        (1,  "Laptop Pro 15",          "Technology",      "Computers",      1299.99),
        (2,  "Wireless Mouse",         "Technology",      "Accessories",      29.99),
        (3,  "USB-C Hub",              "Technology",      "Accessories",      49.99),
        (4,  "Monitor 27-inch",        "Technology",      "Computers",       399.99),
        (5,  "Mechanical Keyboard",    "Technology",      "Accessories",      89.99),
        (6,  "Webcam HD",              "Technology",      "Accessories",      69.99),
        (7,  "External SSD 1TB",       "Technology",      "Storage",         109.99),
        (8,  "Smartphone Stand",       "Technology",      "Accessories",      19.99),
        # Furniture
        (9,  "Ergonomic Chair",        "Furniture",       "Chairs",          349.99),
        (10, "Standing Desk",          "Furniture",       "Tables",          599.99),
        (11, "Bookshelf 5-tier",       "Furniture",       "Bookcases",       149.99),
        (12, "Filing Cabinet",         "Furniture",       "Storage",         199.99),
        (13, "Desk Lamp",              "Furniture",       "Furnishings",      39.99),
        # Office Supplies
        (14, "Ballpoint Pens (12pk)",  "Office Supplies", "Pens",              8.99),
        (15, "Sticky Notes",           "Office Supplies", "Paper",             5.99),
        (16, "Stapler",                "Office Supplies", "Fasteners",        14.99),
        (17, "A4 Paper Ream",          "Office Supplies", "Paper",            12.99),
        (18, "Whiteboard Markers",     "Office Supplies", "Pens",              9.99),
        (19, "Binder A4",              "Office Supplies", "Binders",           6.99),
        (20, "Calculator",             "Office Supplies", "Machines",         24.99),
    ]
    cur.executemany("INSERT INTO products VALUES (?,?,?,?,?)", products_data)

    # Generate orders across 2 years
    start = datetime(2024, 1, 1)
    order_id = 1
    item_id  = 1
    orders_rows, items_rows = [], []

    for _ in range(3_500):
        cust_id    = random.randint(1, 500)
        order_date = start + timedelta(days=random.randint(0, 729))
        ship_date  = order_date + timedelta(days=random.randint(1, 7))
        n_items    = random.randint(1, 5)

        orders_rows.append((
            order_id, cust_id,
            order_date.strftime("%Y-%m-%d"),
            ship_date.strftime("%Y-%m-%d"),
            "Completed"
        ))

        prods = random.sample(range(1, 21), min(n_items, 20))
        for pid in prods:
            price    = dict(products_data)[pid][-1]
            qty      = random.randint(1, 10)
            discount = random.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20])
            revenue  = round(price * qty * (1 - discount), 2)
            items_rows.append((item_id, order_id, pid, qty, discount, revenue))
            item_id += 1

        order_id += 1

    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)",     orders_rows)
    cur.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", items_rows)

    conn.commit()
    conn.close()
    print(f"[✓] Database created → {DB_PATH}")
    print(f"    Orders: {len(orders_rows):,}  |  Line items: {len(items_rows):,}")


# =============================================================================
# 2.  SQL ANALYTICS QUERIES
# =============================================================================

def run_queries(conn) -> dict:
    """Execute all KPI queries and return DataFrames."""
    results = {}

    # ── Q1: Monthly Revenue Trend ─────────────────────────────────────────────
    results["monthly_revenue"] = pd.read_sql_query("""
        SELECT
            strftime('%Y-%m', o.order_date)          AS month,
            ROUND(SUM(oi.revenue), 2)                AS total_revenue,
            COUNT(DISTINCT o.order_id)               AS total_orders,
            ROUND(AVG(oi.revenue), 2)                AS avg_order_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY month
        ORDER BY month
    """, conn)

    # ── Q2: Top 10 Products by Revenue ───────────────────────────────────────
    results["top_products"] = pd.read_sql_query("""
        SELECT
            p.product_name,
            p.category,
            p.sub_category,
            SUM(oi.quantity)                         AS units_sold,
            ROUND(SUM(oi.revenue), 2)                AS total_revenue,
            ROUND(AVG(oi.discount) * 100, 1)         AS avg_discount_pct
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY p.product_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """, conn)

    # ── Q3: Regional Sales Performance ───────────────────────────────────────
    results["regional_sales"] = pd.read_sql_query("""
        SELECT
            r.region_name,
            COUNT(DISTINCT o.order_id)               AS total_orders,
            COUNT(DISTINCT o.customer_id)            AS unique_customers,
            ROUND(SUM(oi.revenue), 2)                AS total_revenue,
            ROUND(SUM(oi.revenue) * 1.0 /
                  SUM(SUM(oi.revenue)) OVER () * 100, 1) AS revenue_share_pct
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN customers   c  ON o.customer_id = c.customer_id
        JOIN regions     r  ON c.region_id   = r.region_id
        GROUP BY r.region_id
        ORDER BY total_revenue DESC
    """, conn)

    # ── Q4: Category Revenue Breakdown ───────────────────────────────────────
    results["category_revenue"] = pd.read_sql_query("""
        WITH category_totals AS (
            SELECT
                p.category,
                ROUND(SUM(oi.revenue), 2)            AS total_revenue,
                SUM(oi.quantity)                     AS units_sold
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            GROUP BY p.category
        )
        SELECT
            category,
            total_revenue,
            units_sold,
            ROUND(total_revenue * 100.0 /
                  SUM(total_revenue) OVER (), 1)     AS revenue_share_pct
        FROM category_totals
        ORDER BY total_revenue DESC
    """, conn)

    # ── Q5: Customer Segment KPIs ─────────────────────────────────────────────
    results["segment_kpis"] = pd.read_sql_query("""
        SELECT
            c.segment,
            COUNT(DISTINCT o.order_id)               AS total_orders,
            COUNT(DISTINCT o.customer_id)            AS unique_customers,
            ROUND(SUM(oi.revenue), 2)                AS total_revenue,
            ROUND(SUM(oi.revenue) /
                  COUNT(DISTINCT o.order_id), 2)     AS avg_order_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN customers   c  ON o.customer_id = c.customer_id
        GROUP BY c.segment
        ORDER BY total_revenue DESC
    """, conn)

    # ── Q6: Month-over-Month Growth (Window Function) ─────────────────────────
    results["mom_growth"] = pd.read_sql_query("""
        WITH monthly AS (
            SELECT
                strftime('%Y-%m', o.order_date)      AS month,
                ROUND(SUM(oi.revenue), 2)            AS revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY month
        )
        SELECT
            month,
            revenue,
            LAG(revenue) OVER (ORDER BY month)       AS prev_revenue,
            ROUND(
                (revenue - LAG(revenue) OVER (ORDER BY month))
                / LAG(revenue) OVER (ORDER BY month) * 100, 1
            )                                        AS mom_growth_pct
        FROM monthly
        ORDER BY month
    """, conn)

    # ── Q7: Running Total Revenue (Window Function) ───────────────────────────
    results["running_total"] = pd.read_sql_query("""
        WITH monthly AS (
            SELECT
                strftime('%Y-%m', o.order_date)      AS month,
                ROUND(SUM(oi.revenue), 2)            AS monthly_revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY month
        )
        SELECT
            month,
            monthly_revenue,
            ROUND(SUM(monthly_revenue) OVER (
                ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2)                                    AS running_total
        FROM monthly
        ORDER BY month
    """, conn)

    # ── Q8: Top Customers by Revenue (CTE) ───────────────────────────────────
    results["top_customers"] = pd.read_sql_query("""
        WITH customer_revenue AS (
            SELECT
                c.customer_id,
                c.customer_name,
                c.segment,
                r.region_name,
                ROUND(SUM(oi.revenue), 2)            AS total_revenue,
                COUNT(DISTINCT o.order_id)           AS total_orders
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN customers   c  ON o.customer_id = c.customer_id
            JOIN regions     r  ON c.region_id   = r.region_id
            GROUP BY c.customer_id
        ),
        ranked AS (
            SELECT *,
                RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
            FROM customer_revenue
        )
        SELECT * FROM ranked WHERE revenue_rank <= 10
    """, conn)

    print("[✓] All 8 SQL queries executed successfully")
    return results


# =============================================================================
# 3.  KPI SUMMARY
# =============================================================================

def print_kpis(data: dict):
    mr  = data["monthly_revenue"]
    reg = data["regional_sales"]
    cat = data["category_revenue"]

    total_rev    = mr["total_revenue"].sum()
    total_orders = mr["total_orders"].sum()
    avg_order    = total_rev / total_orders
    top_region   = reg.iloc[0]["region_name"]
    top_category = cat.iloc[0]["category"]

    print("\n" + "="*55)
    print("   SALES KPI SUMMARY")
    print("="*55)
    print(f"  Total Revenue     :  ${total_rev:>12,.2f}")
    print(f"  Total Orders      :  {total_orders:>12,}")
    print(f"  Avg Order Value   :  ${avg_order:>12,.2f}")
    print(f"  Top Region        :  {top_region}")
    print(f"  Top Category      :  {top_category}")
    print("="*55)


# =============================================================================
# 4.  MATPLOTLIB DASHBOARD  (Power BI simulation)
# =============================================================================

def build_dashboard(data: dict):
    fig = plt.figure(figsize=(20, 14), facecolor=PALETTE["light"])
    fig.suptitle(
        "Sales Performance Dashboard  |  2024 – 2025",
        fontsize=18, fontweight="bold", color=PALETTE["dark"], y=0.98
    )

    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    # ── Panel 1: KPI Scorecards ───────────────────────────────────────────────
    ax_kpi = fig.add_subplot(gs[0, :])
    ax_kpi.set_facecolor(PALETTE["light"])
    ax_kpi.axis("off")

    mr         = data["monthly_revenue"]
    total_rev  = mr["total_revenue"].sum()
    total_ord  = mr["total_orders"].sum()
    avg_ov     = total_rev / total_ord
    top_region = data["regional_sales"].iloc[0]["region_name"]

    kpis = [
        ("💰  Total Revenue",    f"${total_rev:,.0f}",  PALETTE["blue"]),
        ("🛒  Total Orders",     f"{total_ord:,}",      PALETTE["green"]),
        ("📦  Avg Order Value",  f"${avg_ov:,.2f}",     PALETTE["orange"]),
        ("🏆  Top Region",       top_region,             PALETTE["purple"]),
    ]
    for i, (label, val, color) in enumerate(kpis):
        x = 0.12 + i * 0.25
        ax_kpi.add_patch(plt.Rectangle((x-0.10, 0.05), 0.20, 0.85,
                                        transform=ax_kpi.transAxes,
                                        color="white", zorder=1,
                                        linewidth=1.5,
                                        ec=color))
        ax_kpi.text(x, 0.72, label, transform=ax_kpi.transAxes,
                    ha="center", fontsize=10, color=PALETTE["gray"], zorder=2)
        ax_kpi.text(x, 0.35, val,   transform=ax_kpi.transAxes,
                    ha="center", fontsize=16, fontweight="bold", color=color, zorder=2)

    # ── Panel 2: Monthly Revenue Trend ────────────────────────────────────────
    ax1 = fig.add_subplot(gs[1, :2])
    ax1.set_facecolor("white")
    months = data["monthly_revenue"]["month"]
    rev    = data["monthly_revenue"]["total_revenue"]
    ax1.fill_between(range(len(months)), rev, alpha=0.15, color=PALETTE["blue"])
    ax1.plot(range(len(months)), rev, color=PALETTE["blue"], lw=2.5, marker="o",
             markersize=4, label="Monthly Revenue")
    # 3-month rolling avg
    roll = rev.rolling(3, min_periods=1).mean()
    ax1.plot(range(len(months)), roll, color=PALETTE["orange"], lw=2,
             linestyle="--", label="3-Month Rolling Avg")
    tick_positions = list(range(0, len(months), 3))
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels([months.iloc[i] for i in tick_positions], rotation=35, ha="right", fontsize=8)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
    ax1.set_title("Monthly Revenue Trend", fontweight="bold", color=PALETTE["dark"])
    ax1.legend(fontsize=9)
    ax1.set_xlabel("Month"); ax1.set_ylabel("Revenue")

    # ── Panel 3: Category Revenue (Donut) ─────────────────────────────────────
    ax2 = fig.add_subplot(gs[1, 2])
    ax2.set_facecolor("white")
    cat_df  = data["category_revenue"]
    colors  = [PALETTE["blue"], PALETTE["green"], PALETTE["orange"]]
    wedges, texts, autotexts = ax2.pie(
        cat_df["total_revenue"],
        labels=cat_df["category"],
        colors=colors,
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops={"width": 0.55, "edgecolor": "white", "linewidth": 2},
        textprops={"fontsize": 9}
    )
    ax2.set_title("Revenue by Category", fontweight="bold", color=PALETTE["dark"])

    # ── Panel 4: Top 8 Products ───────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[2, :2])
    ax3.set_facecolor("white")
    top8    = data["top_products"].head(8)
    y_pos   = range(len(top8))
    h_bars  = ax3.barh(y_pos, top8["total_revenue"],
                        color=[PALETTE["blue"] if c == "Technology"
                               else PALETTE["green"] if c == "Furniture"
                               else PALETTE["orange"] for c in top8["category"]],
                        edgecolor="white", height=0.6)
    ax3.set_yticks(list(y_pos))
    ax3.set_yticklabels(
        [n[:28] + "…" if len(n) > 28 else n for n in top8["product_name"]],
        fontsize=8.5
    )
    ax3.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
    ax3.set_title("Top 8 Products by Revenue", fontweight="bold", color=PALETTE["dark"])
    ax3.set_xlabel("Revenue")
    ax3.invert_yaxis()
    for bar, val in zip(h_bars, top8["total_revenue"]):
        ax3.text(bar.get_width() + 200, bar.get_y() + bar.get_height()/2,
                 f"${val:,.0f}", va="center", fontsize=8, color=PALETTE["gray"])

    # ── Panel 5: Regional Sales (Bar) ─────────────────────────────────────────
    ax4 = fig.add_subplot(gs[2, 2])
    ax4.set_facecolor("white")
    reg_df  = data["regional_sales"].sort_values("total_revenue")
    bar_cols = sns.color_palette("Blues_d", len(reg_df))
    bars = ax4.barh(reg_df["region_name"], reg_df["total_revenue"],
                    color=bar_cols, edgecolor="white")
    ax4.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
    ax4.set_title("Revenue by Region", fontweight="bold", color=PALETTE["dark"])
    ax4.set_xlabel("Revenue")
    for bar, pct in zip(bars, reg_df.sort_values("total_revenue")["revenue_share_pct"]):
        ax4.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2,
                 f"{pct}%", va="center", fontsize=8.5, color=PALETTE["gray"])

    out = os.path.join(OUT_DIR, "sales_dashboard.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=PALETTE["light"])
    print(f"[✓] Dashboard saved → {out}")
    plt.show()


# =============================================================================
# 5.  CSV EXPORTS
# =============================================================================

def export_csvs(data: dict):
    exports = {
        "monthly_revenue":   data["monthly_revenue"],
        "top_products":      data["top_products"],
        "regional_sales":    data["regional_sales"],
        "category_revenue":  data["category_revenue"],
        "segment_kpis":      data["segment_kpis"],
        "mom_growth":        data["mom_growth"],
        "running_total":     data["running_total"],
        "top_customers":     data["top_customers"],
    }
    for name, df in exports.items():
        path = os.path.join(OUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"[✓] CSV exported → {path}")


# =============================================================================
# 6.  MAIN
# =============================================================================

def main():
    print("\n" + "="*60)
    print("   SALES DASHBOARD & SQL ANALYTICS PIPELINE")
    print("="*60 + "\n")

    # Step 1 — Build DB
    print("[Step 1] Setting up SQLite database …")
    setup_database()

    # Step 2 — Run Queries
    print("\n[Step 2] Running SQL analytics queries …")
    conn    = sqlite3.connect(DB_PATH)
    data    = run_queries(conn)
    conn.close()

    # Step 3 — KPI Summary
    print("\n[Step 3] KPI Summary:")
    print_kpis(data)

    # Step 4 — Dashboard
    print("\n[Step 4] Building Matplotlib dashboard …")
    build_dashboard(data)

    # Step 5 — CSV Exports
    print("\n[Step 5] Exporting CSVs …")
    export_csvs(data)

    print("\n" + "="*60)
    print("  ✓  Pipeline complete")
    print("="*60)
    print(f"\n  Database  : {DB_PATH}")
    print(f"  Dashboard : {OUT_DIR}/sales_dashboard.png")
    print(f"  CSVs      : {OUT_DIR}/*.csv")


if __name__ == "__main__":
    main()
