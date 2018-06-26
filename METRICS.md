# Metric Descriptions

#### User Activity

| Metric name / Code Ref | Description |
|------------------------------|-------------|
| Yearly Active User / `YAU` | The number of clients who used Firefox in the past 365 days. |
| Monthly Active Users / `MAU` | The number of clients who used Firefox in the past 28 days. |
| Daily Usage / `avg_daily_usage(hours)` | Average daily use of a typical client from the past 7 days. Calculated by getting the average daily use for each client from the last week (on days they used), and then averaging across all clients. |
| Average Intensity / `avg_intensity` | Average daily intensity of use of a typical client from the past 7 days. Intensity of use is defined as the proportion of the time a client is interacting with the browser when the browser is open. Calculated by getting the average daily intensity for each client from the last week (on days they used), and then averaging across all clients. |
| New User Rate / `pct_new_user` | Percentage of WAU (clients who used Firefox in the past 7 days) that are new clients (created profile that week). |
| Latest Version / `pct_latest_version` | Percentage of WAU on the newest version (or newer) of Firefox (for that week). Note, Firefox updates are often released with different throttling rates (i.e. 10% of population in week 1, etc.). |

#### Usage Behavior

| Metric name / Code Ref | Description |
|------------------------------|-------------|
| Top Languages / `locale, locale, pct_on_locale` | Percentage of WAU on each language setting (locale). Top 5 per week only. |
| Always On Tracking Protection / `pct_TP` | Percentage of WAU with Always On Tracking Protection enabled for default browsing. Note, this pref was not exposed to users until Firefox 57 (2017-11-14) and does not include Private Browsing Mode. | 
| Has Add-on / `pct_addon` | Percentage of WAU with at least 1 user installed addon. | 
| Top Add-ons / `top10addons, addon_name, pct_with_addon` | The top 10 most common user installed addons from the last 7 days. |



