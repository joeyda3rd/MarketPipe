from datetime import datetime, timedelta

today = datetime.now()
print(f"Today: {today.strftime('%Y-%m-%d')}")

# Stay safely within 730 day limit
two_years_ago = today - timedelta(days=700)
one_week_later = two_years_ago + timedelta(days=5)

print(f"Safe date range:")
print(f"Start: {two_years_ago.strftime('%Y-%m-%d')}")
print(f"End: {one_week_later.strftime('%Y-%m-%d')}")
print(f"Days ago: {(today - two_years_ago).days}")

# Also try a more recent range
recent_start = today - timedelta(days=30)
recent_end = today - timedelta(days=25)
print(f"\nMore recent range:")
print(f"Start: {recent_start.strftime('%Y-%m-%d')}")  
print(f"End: {recent_end.strftime('%Y-%m-%d')}")
print(f"Days ago: {(today - recent_start).days}") 