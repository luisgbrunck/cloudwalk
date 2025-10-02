# CloudWalk Monitoring Challenge

This repository contains the implementation of a **monitoring dashboard** designed to simulate the tracking of transactions and authorization codes (`auth_codes`) from public CSV files.

The application is deployed and available online at:

👉 [https://www.melhoresempregos.com/cloud/cloudwalk.php](https://www.melhoresempregos.com/cloud/cloudwalk.php)

---

## 📌 Overview

The project recreates a simplified **Command Center / NOC environment with a simple system of Observability**, where:

- CSVs from different sources (transactions, checkouts, authorization codes) are downloaded automatically;
- Data is imported and normalized into MySQL;
- Dashboards built with **PHP + Bootstrap + Chart.js** display metrics such as transactions per minute, checkout comparisons, and automatic alerts with configurable thresholds.

---

## 🚀 Key Challenges

### 1. Importing CSVs with Different Schemas
- The `transactions_auth_codes.csv` file initially failed to populate the `auth_codes` table.  
- Root cause: the CSV only contained `timestamp, auth_code, count` — with no `status` column.  
- Solution: implemented an **aggregate mode** to expand the `count` column into multiple rows, storing `timestamp` and `auth_code` with `status=NULL`.

### 2. Idealization Of Thresholds Customizations
- WOW, that was hard to do, and I'm thinking if this is was the best way to do that.

### 3. Time To Do And My Grandma Visiting Me after 1 Year
- I needed to do with some help of chatgpt and during the night before sleeping. I was traveling with her.

---

## 🛠️ Tech Stack
- **PHP 8.3**
- **MySQL / MariaDB**
- **Bootstrap 5**
- **Chart.js + Plugins (Zoom, Annotation)**
- **cURL / file_get_contents with custom contexts**

---

## 📊 Features
- Dashboard with transactions per minute (line, area, bar, stacked).
- Checkout comparison per hour.
- Alerts list with filtering and pagination.
- Dynamic threshold configuration for alerts.
- Buttons to **reload data** and **trigger alert scans** manually.

---

## 📍 Access
The live system is available at:

🔗 [https://www.melhoresempregos.com/cloud/cloudwalk.php](https://www.melhoresempregos.com/cloud/cloudwalk.php)

---

## 📑 License
This project was created for educational and technical demonstration purposes.
