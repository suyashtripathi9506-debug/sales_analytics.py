# 📊 Sales Dashboard & SQL Analytics

> End-to-end sales analytics pipeline using **Python**, **SQLite**, **Pandas**, and **Matplotlib** — featuring 12 advanced SQL queries, a Power BI-style dashboard, and automated CSV exports across a synthetic dataset of 3,500+ orders.

---

## 📌 Project Overview

| Detail | Info |
|---|---|
| **Domain** | Sales / Business Intelligence |
| **Database** | SQLite (`sales.db`) |
| **Records** | 3,500+ orders · 500 customers · 20 products · 5 regions |
| **SQL Techniques** | JOINs, CTEs, Window Functions (LAG, RANK, SUM OVER) |
| **Dashboard Visuals** | 6 panels (KPI cards, trend line, donut, bar charts) |
| **Timeline** | Aug 2025 – Sep 2025 |

---

## 🗂️ Project Structure

```
sales-analytics/
│
├── sales_analytics.py       # Full pipeline (DB setup → queries → dashboard → CSV)
├── sales_queries.sql        # 12 standalone annotated SQL queries
│
├── outputs/
│   ├── sales_dashboard.png  # Matplotlib Power BI-style dashboard
│   ├── monthly_revenue.csv
│   ├── top_products.csv
│   ├── regional_sales.csv
│   ├── category_revenue.csv
│   ├── segment_kpis.csv
│   ├── mom_growth.csv
│   ├── running_total.csv
│   └── top_customers.csv
│
└── README.md
```

---

## ⚙️ Pipeline Steps

```
1. Database Setup      →  SQLite schema (5 tables) + 3,500 synthetic orders
2. SQL Queries         →  8 KPI queries via pd.read_sql_query()
3. KPI Summary         →  Console printout of key business metrics
4. Dashboard           →  6-panel Matplotlib dashboard (Power BI simulation)
5. CSV Exports         →  8 structured CSV files saved to outputs/
```

---

## 🧰 Tech Stack

| Library | Purpose |
|---|---|
| `sqlite3` | Relational database engine |
| `pandas` | Data manipulation and SQL result handling |
| `matplotlib` | Dashboard and chart rendering |
| `seaborn` | Styling and color palettes |
| `numpy` | Synthetic data generation |

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/sales-analytics.git
cd sales-analytics
```

### 2. Install dependencies

```bash
pip install pandas matplotlib seaborn numpy
```

> Python 3.8+ recommended. No database server required — SQLite is built into Python.

### 3. Run the pipeline

```bash
python sales_analytics.py
```

The script will:
- Create and populate `sales.db` automatically
- Execute all 8 SQL queries
- Print KPI summary to the console
- Save the dashboard to `outputs/sales_dashboard.png`
- Export 8 CSV files to `outputs/`

### 4. Run SQL queries independently

Open `sales_queries.sql` in any SQLite-compatible tool (DB Browser for SQLite, DBeaver, VSCode SQLite extension) and point it at `sales.db`.

---

## 📊 Dashboard Panels

| Panel | Visual Type | KPI |
|---|---|---|
| KPI Scorecards | Metric cards | Total Revenue, Orders, Avg Order Value, Top Region |
| Monthly Revenue Trend | Line + fill | Revenue over time + 3-month rolling avg |
| Revenue by Category | Donut chart | Technology / Furniture / Office Supplies share |
| Top 8 Products | Horizontal bar | Revenue ranked by product |
| Revenue by Region | Horizontal bar | North / South / East / West / Central |

---

## 🗄️ Database Schema

```sql
regions     (region_id, region_name)
customers   (customer_id, customer_name, email, region_id, segment)
products    (product_id, product_name, category, sub_category, unit_price)
orders      (order_id, customer_id, order_date, ship_date, status)
order_items (item_id, order_id, product_id, quantity, discount, revenue)
```

---

## 🔍 SQL Techniques Used

**Joins**
```sql
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers   c  ON o.customer_id = c.customer_id
JOIN regions     r  ON c.region_id   = r.region_id
```

**CTEs (Common Table Expressions)**
```sql
WITH customer_revenue AS (
    SELECT customer_id, SUM(revenue) AS total_revenue ...
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)
SELECT * FROM ranked WHERE revenue_rank <= 10;
```

**Window Functions**
```sql
-- Month-over-Month Growth
LAG(revenue) OVER (ORDER BY month)

-- Running Total
SUM(monthly_revenue) OVER (
    ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)

-- Rank within Region
RANK() OVER (PARTITION BY region_id ORDER BY SUM(revenue) DESC)
```

---

## 📈 Sample KPIs (Synthetic Data)

| KPI | Value |
|---|---|
| Total Revenue | ~$2.8M |
| Total Orders | 3,500+ |
| Avg Order Value | ~$800 |
| Top Category | Technology |
| Top Region | North / East (varies by seed) |

---

## 🔭 Future Improvements

- [ ] Connect to a live PostgreSQL or MySQL database
- [ ] Build an interactive Power BI / Tableau dashboard
- [ ] Add a Streamlit web app for live query exploration
- [ ] Schedule automated reports with Apache Airflow
- [ ] Integrate real Kaggle sales datasets (e.g. Superstore)

---

## 👤 Author

**Your Name**
[LinkedIn](https://linkedin.com/in/your-profile) · [GitHub](https://github.com/your-username)

---

## 📄 License

This project is licensed under the MIT License.
