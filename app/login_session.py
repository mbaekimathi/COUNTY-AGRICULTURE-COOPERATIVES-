"""Login-session segments for HR time tracking: count active use; pause after idle."""

# Flask session key matching persisted employee_login_sessions.id (current open segment).
LOGIN_SESSION_PK = "login_session_pk"


def touch_login_session_for_current_request(app) -> None:
    """
    While signed in, advance \"live\" session time only for active use.

    - Each request refreshes last_seen_at when still within the idle window.
    - After SESSION_ACTIVITY_IDLE_MINUTES without a request (no heartbeat), the segment is
      closed at last_seen_at + idle window (paused); the next request starts a new segment.

    Static asset requests are skipped to reduce DB writes.
    """
    from flask import request, session

    from app.db import get_connection

    employee_id = session.get("employee_id")
    login_pk = session.get(LOGIN_SESSION_PK)
    if not employee_id or not login_pk:
        return

    endpoint = request.endpoint
    if endpoint == "static" or (request.path or "").startswith("/static"):
        return
    # Avoid DB writes on stylesheet and login-code availability polling.
    if endpoint in {"main.app_stylesheet", "auth.check_login_code"}:
        return

    idle_minutes = max(1, int(app.config.get("SESSION_ACTIVITY_IDLE_MINUTES", 15)))
    idle_seconds_threshold = idle_minutes * 60

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, ended_at
                FROM employee_login_sessions
                WHERE id = %s AND employee_id = %s
                LIMIT 1
                """,
                (login_pk, employee_id),
            )
            row = cur.fetchone()
            if not row or row.get("ended_at") is not None:
                return

            cur.execute(
                """
                SELECT TIMESTAMPDIFF(SECOND, last_seen_at, NOW()) AS idle_secs
                FROM employee_login_sessions
                WHERE id = %s AND ended_at IS NULL
                """,
                (login_pk,),
            )
            ir = cur.fetchone()
            idle_secs = int(ir["idle_secs"] or 0) if ir else 0

            if idle_secs > idle_seconds_threshold:
                cur.execute(
                    """
                    UPDATE employee_login_sessions
                    SET ended_at = TIMESTAMPADD(MINUTE, %s, last_seen_at)
                    WHERE id = %s AND employee_id = %s AND ended_at IS NULL
                    """,
                    (idle_minutes, login_pk, employee_id),
                )
                cur.execute(
                    """
                    INSERT INTO employee_login_sessions (employee_id, started_at, last_seen_at)
                    VALUES (%s, NOW(), NOW())
                    """,
                    (employee_id,),
                )
                session[LOGIN_SESSION_PK] = cur.lastrowid
            else:
                cur.execute(
                    """
                    UPDATE employee_login_sessions
                    SET last_seen_at = NOW()
                    WHERE id = %s AND employee_id = %s AND ended_at IS NULL
                    """,
                    (login_pk, employee_id),
                )
    finally:
        conn.close()
