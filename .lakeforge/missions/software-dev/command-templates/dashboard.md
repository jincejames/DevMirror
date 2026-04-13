---
description: Open the Lakeforge dashboard in your browser.
---

## Dashboard Access

This command launches the Lakeforge dashboard in your browser using the lakeforge CLI.

## What to do

Simply run the `lakeforge dashboard` command to:
- Start the dashboard if it's not already running
- Open it in your default web browser
- Display the dashboard URL

If you need to stop the dashboard, you can use `lakeforge dashboard --kill`.

## Implementation

Execute the following terminal command:

```bash
lakeforge dashboard
```

## Additional Options

- To specify a preferred port: `lakeforge dashboard --port 8080`
- To stop the dashboard: `lakeforge dashboard --kill`

## Success Criteria

- User sees the dashboard URL clearly displayed
- Browser opens automatically to the dashboard
- If browser doesn't open, user gets clear instructions
- Error messages are helpful and actionable