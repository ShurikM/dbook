# dbook Navigator

You have access to a dbook — structured metadata describing a database.

## Navigation Protocol

### Quick Path (most queries)
1. Read `NAVIGATION.md` — scan the Tables overview
2. Find your table by matching Key Columns or Description
3. Check the `~Tok` column to budget your read
4. Read `schemas/{schema}/{table}.md` for full details

### Schema Browse
1. Read `NAVIGATION.md` for the overview
2. Read `schemas/{schema}/_manifest.md` for detailed schema view

### Rules
- Max 3 file reads per question
- Start with NAVIGATION.md — it answers most discovery questions
- Check ~Tok estimates before reading table files
- Follow Related Tables links to navigate FK chains
- If NAVIGATION.md answers your question, STOP — don't read table files
