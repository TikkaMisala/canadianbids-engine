"""
summarizer.py — Generates plain-English summaries for tenders using Claude API.

Summaries are stored in tenders.ai_summary column.
Only generates summaries for tenders that don't have one yet.
"""

import anthropic
import time


SUMMARY_PROMPT = """You are summarizing a Canadian federal government procurement tender for a small business owner.

Write exactly 2 sentences:
1. What the government is buying (be specific — include the department, the product/service, and any key details like location, duration, or quantity).
2. What type of supplier should apply (be specific about required expertise, certifications, location, or business size).

Be direct and concrete. Avoid bureaucratic language. Do not start with "The" for both sentences.

Tender details:
Title: {title}
Department: {department}
Category: {category}
Region: {region}
Notice type: {notice_type}
Procurement method: {procurement_method}
Selection criteria: {selection_criteria}
Description: {description}

Write only the 2-sentence summary. No preamble, no labels, no bullet points."""


def generate_summary(client, tender):
    """Generate a plain-English summary for a single tender."""
    desc = (tender.get("description") or "").strip()
    if not desc:
        return None

    prompt = SUMMARY_PROMPT.format(
        title=tender.get("title") or "",
        department=tender.get("department") or "",
        category=tender.get("category") or "",
        region=tender.get("region") or "",
        notice_type=tender.get("notice_type") or "",
        procurement_method=tender.get("procurement_method") or "",
        selection_criteria=tender.get("selection_criteria") or "",
        description=desc[:2000],  # truncate very long descriptions
    )

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",  # Fast and cheap for summaries
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        summary = message.content[0].text.strip()
        return summary
    except Exception as e:
        print(f"  Claude API error: {e}")
        return None


def run_summarizer(db, anthropic_key, batch_size=50):
    """
    Generates AI summaries for tenders that don't have one yet.
    Processes in batches to avoid rate limits.
    """
    print(f"\n{'='*50}")
    print("Starting summarizer job...")
    print(f"{'='*50}")

    client = anthropic.Anthropic(api_key=anthropic_key)

    # Load tenders without summaries
    resp = db.table("tenders") \
        .select("id, title, department, category, region, notice_type, procurement_method, selection_criteria, description") \
        .is_("ai_summary", "null") \
        .limit(batch_size) \
        .execute()

    tenders = resp.data or []
    print(f"Tenders without summaries: {len(tenders)}")

    if not tenders:
        print("All tenders already have summaries.")
        return {"summarized": 0, "errors": 0}

    summarized = 0
    errors = 0

    for i, tender in enumerate(tenders):
        tender_id = tender.get("id")
        title = (tender.get("title") or "")[:60]
        print(f"  [{i+1}/{len(tenders)}] {title}...")

        summary = generate_summary(client, tender)

        if summary:
            try:
                db.table("tenders") \
                    .update({"ai_summary": summary}) \
                    .eq("id", tender_id) \
                    .execute()
                summarized += 1
                print(f"    ✓ {summary[:80]}...")
            except Exception as e:
                errors += 1
                print(f"    DB error: {e}")
        else:
            errors += 1

        # Rate limit: ~3 requests/sec for Haiku
        if (i + 1) % 10 == 0:
            time.sleep(1)

    print(f"\nSummarizer complete.")
    print(f"  Summarized: {summarized}")
    print(f"  Errors: {errors}")

    return {"summarized": summarized, "errors": errors}
