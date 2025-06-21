# 🌍 Google Maps Review Scraper - Long-Running Infrastructure on AWS with Apify + Supabase


## 🧠 Project Goal

The primary goal of this project was to **scrape Google Maps reviews** reliably and without timeouts, **regardless of duration**, and persist the data into a **Supabase Postgres database**. We needed a scalable backend that could:

- Trigger scraping with parameters like `placeIds` or `search queries`.
- Process & clean data in real time.
- Handle **long-running scraping jobs** with **zero timeout**.
- Work across **AWS Elastic Beanstalk**, **Apify**, and **Supabase**.

---

## 🔧 Tech Stack

- **AWS Elastic Beanstalk** – for deploying long-running Python apps with persistent runtime.
- **Apify** – to power the actual Google Maps review scraping.
- **Supabase** – for real-time database operations via REST and RPC.
- **Python 3.9+** – core backend logic.
- **PostgreSQL** – structured schema for businesses, reviews, and users.

---

## 💡 Features

- ✅ **Unlimited scraping runtime** – no timeout errors thanks to Elastic Beanstalk.
- ✅ **Clean data pipeline** from Apify → Python → Supabase.
- ✅ **Automatic error handling** and logging.
- ✅ **Asynchronous scraping** via Lambda-style `returnImmediately` support.
- ✅ **Review sentiment tagging**, categorization & structured storage.
- ✅ **RESTful scraping metadata & status tracking**.

---

## ⚠️ Problems We Encountered

| Issue | Resolution |
|------|------------|
| ❌ Supabase 401 Unauthorized | We fixed broken or missing API keys by verifying `.env` configuration inside EB. |
| ❌ `user_profile_id` was required but missing | We enforced user tracking in the request and updated Supabase relationships to reference `int8`. |
| ❌ Scraping returned no results | Fixed by reviewing `placeIds`, `searchStringsArray`, and `minReviewDate`. |
| ❌ Invalid ZIP on deploy | Redeployed using `eb deploy` with a valid `.zip` archive. |
| ❌ Business insert conflicts | We added unique constraints and handled duplicate entries gracefully. |

---

## 🚀 How to Run

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/gmaps-review-scraper.git
cd gmaps-review-scraper
```
2. Add .env file
```
SUPABASE_URL=https://<your-project>.supabase.co
SUPABASE_KEY=your-service-role-key
APIFY_TOKEN=your-apify-token
```
3. Run Locally
```
python3 index.py
```
This simulates a Lambda-style run with a fake event for testing.

4. Run in Production (via API)
Make a POST request to:
```
POST http://your-beanstalk-env.elasticbeanstalk.com/run
```
Body Example:
```
{
  "placeIds": ["ChIJN1t_tDeuEmsRUsoyG83frY4"],
  "user_profile_id": 2,
  "scraping_metadata_id": "a9b1f2f5-afda-4d7e-8eea-8512a72bfc94",
  "maxReviews": 10,
  "returnImmediately": false
}
```

🧾 Supabase Schema (Simplified)

auth_users: stores user auth details.

user_profile: tracks personal profiles (referenced via int8).

businesses, branches, reviews: core entities.

scraping_metadata: logs each scraping attempt and results.

process_google_review: stored procedure for inserting review JSON.



## 📌 Final Thoughts
This project reinforced the importance of environment consistency, clean parameter validation, and clear Supabase role/auth control. With zero timeouts, modular scraping, and full control over data flow — we now have a scalable system that can handle thousands of reviews, across the globe, reliably.

Feel free to ⭐️ this repo or fork it if you're building scraping pipelines!

---

