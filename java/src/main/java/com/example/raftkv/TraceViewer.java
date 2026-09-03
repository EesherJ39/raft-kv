package com.example.raftkv;

/** Dependency-free diagnostic UI served by every node. */
final class TraceViewer {
    private TraceViewer() {}

    static String page() {
        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>RaftKV Trace</title>
                  <style>
                    :root{color-scheme:dark;--bg:#07111f;--panel:#0e1d31;--line:#253d59;--ink:#edf6ff;--muted:#91a7bd;--accent:#59d3c4;--gold:#ffcb6b}
                    *{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#17324e 0,var(--bg) 38%);color:var(--ink);font:15px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace}
                    main{width:min(1100px,calc(100% - 32px));margin:36px auto}header{display:flex;justify-content:space-between;gap:24px;align-items:end;margin-bottom:22px}
                    h1{font:700 34px/1.1 system-ui,sans-serif;margin:0}.eyebrow{color:var(--accent);letter-spacing:.14em;text-transform:uppercase;font-size:12px}
                    #state{color:var(--muted)}.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:18px}.card,.events{background:color-mix(in srgb,var(--panel) 92%,transparent);border:1px solid var(--line);border-radius:12px}
                    .card{padding:14px}.label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}.value{font-size:21px;margin-top:3px}
                    .events{overflow:hidden}.row{display:grid;grid-template-columns:80px 70px 110px 150px 1fr;gap:10px;padding:9px 13px;border-top:1px solid var(--line)}
                    .row:first-child{border-top:0}.head{color:var(--muted);font-size:11px;text-transform:uppercase}.term{color:var(--gold)}.event{color:var(--accent)}
                    @media(max-width:760px){.cards{grid-template-columns:repeat(2,1fr)}.row{grid-template-columns:55px 55px 100px 1fr}.detail{grid-column:2/-1;color:var(--muted)}header{align-items:start;flex-direction:column}}
                  </style>
                </head>
                <body><main>
                  <header><div><div class="eyebrow">Live protocol diagnostics</div><h1>RaftKV Trace</h1></div><div id="state">connecting...</div></header>
                  <section class="cards" id="cards"></section>
                  <section class="events" id="events"><div class="row head"><span>Seq</span><span>Term</span><span>Role</span><span>Event</span><span>Detail</span></div></section>
                </main>
                <script>
                  const esc=v=>String(v??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
                  async function refresh(){
                    try{
                      const [status,trace]=await Promise.all([fetch("/v1/status").then(r=>r.json()),fetch("/v1/trace").then(r=>r.json())]);
                      state.textContent=`${status.id} / ${status.role}`;
                      const fields=[["term",status.term],["leader",status.leaderId??"unknown"],["commit",status.commitIndex],["snapshot",status.snapshotIndex],["retained log",status.retainedLogEntries]];
                      cards.innerHTML=fields.map(([k,v])=>`<article class="card"><div class="label">${esc(k)}</div><div class="value">${esc(v)}</div></article>`).join("");
                      events.innerHTML='<div class="row head"><span>Seq</span><span>Term</span><span>Role</span><span>Event</span><span>Detail</span></div>'+
                        trace.slice().reverse().map(e=>`<div class="row"><span>${e.sequence}</span><span class="term">${e.term}</span><span>${esc(e.role)}</span><span class="event">${esc(e.event)}</span><span class="detail">${esc(e.detail)}</span></div>`).join("");
                    }catch(error){state.textContent="disconnected"}
                  }
                  refresh();setInterval(refresh,1000);
                </script></body></html>
                """;
    }
}
