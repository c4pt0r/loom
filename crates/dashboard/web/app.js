"use strict";
const $ = (id) => document.getElementById(id);
const phases = [
  "running",
  "queued",
  "waiting",
  "retrying",
  "succeeded",
  "failed",
];
const labels = {
  tasks: "Tasks",
  actors: "Actors",
  nodes: "Nodes",
  messages: "Messages",
};
const descriptions = {
  tasks: "Follow work from submission to committed result.",
  actors: "Inspect durable state, queued messages, and saved results.",
  nodes: "See current registrations, lease health, and claimed work.",
  messages: "Follow actor turns, retries, and committed receipts.",
};
const view = { resource: "tasks", phase: "", search: "", offset: 0, limit: 25 };
let snapshot = null,
  controller = null,
  requestVersion = 0,
  detailVersion = 0,
  lastUpdated = null;
function el(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = text;
  if (cls) n.className = cls;
  return n;
}
function replace(id, ...nodes) {
  $(id).replaceChildren(...nodes);
}
function count(map, key) {
  return map?.[key] || 0;
}
function total(map) {
  return Object.values(map || {}).reduce((a, b) => a + b, 0);
}
function badge(s) {
  return el("span", s[0].toUpperCase() + s.slice(1), "badge " + s);
}
function number(n) {
  return n.toLocaleString();
}
function date(n) {
  return n ? new Date(n).toLocaleString() : "—";
}
function pretty(text) {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}
async function api(path, signal) {
  const response = await fetch(path, { signal, cache: "no-store" });
  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { error: text };
  }
  if (!response.ok)
    throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}
function choose(resource, phase = "") {
  view.resource = resource;
  view.phase = phase;
  view.offset = 0;
  view.search = "";
  $("search").value = "";
  $("phase").value = phase;
  for (const button of document.querySelectorAll(".nav-item")) {
    const active = button.dataset.resource === resource;
    button.classList.toggle("active", active);
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  }
  $("resource-title").textContent = labels[resource];
  $("resource-description").textContent = descriptions[resource];
  $("phase-control").hidden = ["nodes", "actors"].includes(resource);
  $("search").placeholder =
    resource === "nodes"
      ? "Search node ID…"
      : resource === "actors"
        ? "Search actor ID or handler…"
        : "Search ID, function, or actor…";
  // Do not leave rows from a different resource under the new heading while loading.
  replace("table-body");
  replace("table-head");
  $("empty").hidden = true;
  $("page-label").textContent = "Loading resources…";
  $("previous").disabled = true;
  $("next").disabled = true;
  refresh(true);
}
function metrics(data) {
  const s = data.summary,
    t = s.tasks;
  const nodes = count(s.nodes, "online"),
    draining = count(s.nodes, "draining"),
    expired = count(s.nodes, "expired");
  $("metric-nodes").textContent = number(nodes);
  $("metric-running").textContent = number(count(t, "running"));
  const pending =
    count(t, "queued") + count(t, "waiting") + count(t, "retrying");
  $("metric-pending").textContent = number(pending);
  $("metric-failed").textContent = number(count(t, "failed"));
  $("nodes-note").textContent = `${draining} draining · ${expired} expired`;
  $("pending-note").textContent =
    `${count(t, "queued")} queued · ${count(t, "waiting")} waiting · ${count(t, "retrying")} retrying`;
  $("failed-note").textContent = count(t, "failed")
    ? "Open a task to inspect the failure"
    : "No committed task failures";
  $("failed-note").classList.toggle("attention", count(t, "failed") > 0);
  $("nav-tasks").textContent = number(total(t));
  $("nav-actors").textContent = number(s.actors);
  $("nav-nodes").textContent = number(total(s.nodes));
  $("nav-messages").textContent = number(total(s.messages));
  const failures = count(t, "failed") + count(s.messages, "failed");
  const stalled =
    nodes === 0 &&
    (pending > 0 ||
      count(s.messages, "queued") +
        count(s.messages, "retrying") +
        count(s.messages, "waiting") >
        0);
  document
    .querySelector(".health-strip")
    .classList.toggle("warning", stalled || failures > 0);
  $("health-title").textContent = stalled
    ? "Work is waiting for a worker"
    : failures
      ? "Some work needs attention"
      : nodes
        ? "Workers are online"
        : "No active workers";
  $("health-description").textContent = stalled
    ? "Start a compatible worker in this namespace."
    : failures
      ? `${count(t, "failed")} failed tasks · ${count(s.messages, "failed")} failed actor messages. Open a record for the reason.`
      : nodes
        ? "Live registrations are checked against backend time."
        : "Completed results and durable actor state remain available.";
  $("snapshot-time").textContent =
    "SNAPSHOT " + new Date(data.now_ms).toLocaleTimeString();
  $("execution-total").textContent = number(total(t)) + " tasks";
  replace("distribution");
  replace("legend");
  for (const phase of phases) {
    const n = count(t, phase);
    if (n) {
      const segment = el("span", undefined, phase);
      segment.style.flexGrow = String(n);
      segment.title = `${phase}: ${n}`;
      $("distribution").append(segment);
    }
    const button = el("button");
    button.append(
      el("i", undefined, phase),
      el("span", phase[0].toUpperCase() + phase.slice(1)),
      el("b", number(n)),
    );
    button.addEventListener("click", () => choose("tasks", phase));
    $("legend").append(button);
  }
}
function rowButton(title, id, action) {
  const b = el("button", undefined, "row-button");
  b.append(el("strong", title), el("small", id));
  b.title = id;
  b.addEventListener("click", action);
  return b;
}
function cell(content) {
  const td = el("td");
  if (content instanceof Node) td.append(content);
  else td.textContent = content;
  return td;
}
function outputButton(digest, label = "View result ↗") {
  const b = el("button", label, "result-button");
  b.addEventListener("click", () => openObject(digest));
  return b;
}
function renderTable(data) {
  const isJobs = ["tasks", "messages"].includes(view.resource);
  const headers = isJobs
    ? ["Work / ID", "Status", "Attempts", "Worker", "Result"]
    : view.resource === "actors"
      ? [
          "Actor / Handler",
          "Application phase",
          "Revision",
          "Pending messages",
          "Active turns",
        ]
      : ["Node / Incarnation", "Status", "Active jobs", "Lease"];
  const head = el("tr");
  headers.forEach((h) => head.append(el("th", h)));
  replace("table-head", head);
  replace("table-body");
  const items = isJobs
    ? data.jobs
    : view.resource === "actors"
      ? data.actors
      : data.nodes;
  for (const item of items) {
    const tr = el("tr");
    if (isJobs) {
      const status = cell(badge(item.phase));
      status.title = item.reason;
      if (item.error) {
        const note = el(
          "span",
          (item.phase === "failed" ? "" : "Previous attempt: ") + item.error,
          "row-error",
        );
        note.title = item.error;
        status.append(note);
      }
      tr.append(
        cell(
          rowButton(
            item.actor ? `${item.name} · ${item.actor}` : item.name,
            item.id,
            () => openJob(item.id),
          ),
        ),
        status,
        cell(`${item.attempts} / ${item.max_attempts}`),
        cell(item.owner?.node || "—"),
        cell(item.output ? outputButton(item.output.digest) : "—"),
      );
    } else if (view.resource === "actors") {
      let state;
      try {
        state = JSON.parse(item.state.text);
      } catch {}
      let phase =
        typeof state?.phase === "string" && state.phase ? state.phase : "—";
      tr.append(
        cell(rowButton(item.id, item.handler, () => openActor(item.id))),
        cell(phase),
        cell(String(item.revision)),
        cell(String(item.pending)),
        cell(String(item.running)),
      );
    } else {
      const remaining = Math.max(0, item.lease_until - data.now_ms);
      tr.append(
        cell(
          rowButton(item.session.node, item.session.incarnation, () =>
            openNode(item, data.now_ms),
          ),
        ),
        cell(badge(item.status)),
        cell(String(item.running)),
        cell(
          item.status === "expired"
            ? "Expired"
            : `${(remaining / 1000).toFixed(1)}s remaining`,
        ),
      );
    }
    $("table-body").append(tr);
  }
  $("empty").hidden = items.length !== 0;
  $("empty-title").textContent =
    view.search || view.phase
      ? "No matching " + view.resource
      : "No " + view.resource + " yet";
  $("empty-description").textContent =
    view.search || view.phase
      ? "Try another search or clear the status filter."
      : view.resource === "nodes"
        ? "Start a worker connected to this namespace."
        : view.resource === "actors"
          ? "Create a durable actor to inspect its state here."
          : view.resource === "messages"
            ? "Send a message to an actor to see its execution here."
            : "Submit a task to this namespace to see its progress here.";
  $("result-count").textContent = number(data.total) + " records";
  $("page-label").textContent = data.total
    ? `${view.offset + 1}–${Math.min(view.offset + view.limit, data.total)} of ${number(data.total)}`
    : "0 records";
  $("previous").disabled = view.offset === 0;
  $("next").disabled = view.offset + view.limit >= data.total;
}
async function refresh(force = false) {
  if (controller && !force) return;
  if (controller) controller.abort();
  const current = ++requestVersion;
  controller = new AbortController();
  const query = new URLSearchParams({
    resource: view.resource,
    search: view.search,
    offset: String(view.offset),
    limit: String(view.limit),
  });
  if (view.phase) query.set("phase", view.phase);
  try {
    const data = await api("/api/inspect?" + query, controller.signal);
    if (current !== requestVersion) return;
    snapshot = data;
    lastUpdated = new Date();
    metrics(data);
    renderTable(data);
    $("error-banner").hidden = true;
    $("connection").classList.remove("stale");
    $("connection-text").textContent = $("auto").checked
      ? "Live · every 3s"
      : "Auto-refresh paused";
  } catch (e) {
    if (e.name === "AbortError" || current !== requestVersion) return;
    $("error-banner").hidden = false;
    $("error-banner").textContent =
      `${e.message} ${lastUpdated ? "Last successful refresh: " + lastUpdated.toLocaleTimeString() + ". Displayed data is stale." : "No backend snapshot is available."}`;
    $("connection").classList.add("stale");
    $("connection-text").textContent = "Disconnected · data stale";
    $("health-title").textContent = "Backend connection unavailable";
    $("health-description").textContent =
      "Node and task health cannot be confirmed until refresh succeeds.";
    document.querySelector(".health-strip").classList.add("warning");
    if (!snapshot) $("page-label").textContent = "Could not load resources";
  } finally {
    if (current === requestVersion) controller = null;
  }
}
function openPanel(kind, title) {
  const version = ++detailVersion;
  $("detail-kind").textContent = kind;
  $("detail-title").textContent = title;
  replace("detail-content", el("p", "Loading…", "detail-note"));
  if (!$("detail").open) $("detail").showModal();
  return version;
}
function detailLine(label, value) {
  const n = el("div", undefined, "detail-line");
  n.append(el("span", label), el("span", String(value ?? "—")));
  return n;
}
function section(title) {
  const s = el("section", undefined, "detail-section");
  s.append(el("h3", title));
  return s;
}
function preview(title, p) {
  const s = section(title);
  s.append(
    el(
      "p",
      `${number(p.bytes)} bytes${p.truncated ? " · Preview truncated" : ""}`,
      "detail-note",
    ),
    el("pre", pretty(p.text)),
  );
  return s;
}
function references(value, path = "", out = []) {
  if (out.length >= 30) return out;
  if (value && typeof value === "object") {
    if (
      typeof value.digest === "string" &&
      /^[a-f0-9]{64}$/i.test(value.digest)
    ) {
      out.push({ path, digest: value.digest });
    } else
      for (const [k, v] of Object.entries(value)) {
        references(v, path ? `${path}.${k}` : k, out);
        if (out.length >= 30) break;
      }
  }
  return out;
}
async function openJob(id) {
  const version = openPanel("EXECUTION", "Task details");
  try {
    const d = await api("/api/job?" + new URLSearchParams({ id }));
    if (version !== detailVersion) return;
    $("detail-title").textContent = id.startsWith("message:")
      ? "Actor message"
      : "Task details";
    const info = section(d.job.name);
    info.append(
      badge(d.job.phase),
      el("p", d.job.reason, "detail-note"),
      detailLine("ID", d.job.id),
      detailLine("Attempts", `${d.job.attempts} / ${d.job.max_attempts}`),
      detailLine("Worker", d.job.owner?.node),
      detailLine("Incarnation", d.job.owner?.incarnation),
      detailLine("Work lease", date(d.job.lease_until)),
      detailLine("Eligible after", date(d.job.available_at)),
      detailLine("Module", d.module.digest),
    );
    if (d.job.actor) {
      const b = el("button", "Open actor: " + d.job.actor, "reference");
      b.addEventListener("click", () => openActor(d.job.actor));
      info.append(b);
    }
    replace("detail-content", info);
    if (d.job.error) {
      const failure = section(
        d.job.phase === "failed"
          ? "Failure reason"
          : "Last recorded attempt error",
      );
      failure.append(el("div", d.job.error, "detail-failure"));
      $("detail-content").append(failure);
    }
    if (d.job.output) {
      const result = section("Committed result");
      result.append(
        outputButton(d.job.output.digest, "Open result object ↗"),
        detailLine("Digest", d.job.output.digest),
      );
      $("detail-content").append(result);
    }
    $("detail-content").append(preview("Input", d.input));
    if (d.receipt)
      $("detail-content").append(preview("Commit receipt", d.receipt));
  } catch (e) {
    if (version === detailVersion)
      replace("detail-content", el("p", e.message, "detail-failure"));
  }
}
async function openActor(id) {
  const version = openPanel("DURABLE STATE", "Actor details");
  try {
    const a = await api("/api/actor?" + new URLSearchParams({ id }));
    if (version !== detailVersion) return;
    const info = section(a.handler);
    info.append(
      detailLine("Actor", a.id),
      detailLine("Revision", a.revision),
      detailLine("Pending messages", a.pending),
      detailLine("Active turns", a.running),
    );
    replace(
      "detail-content",
      info,
      preview("Committed application state", a.state),
    );
    try {
      const refs = references(JSON.parse(a.state.text));
      if (refs.length) {
        const outputs = section("Referenced objects");
        for (const r of refs) {
          const b = el("button", `${r.path} ↗`, "reference");
          b.title = r.digest;
          b.addEventListener("click", () => openObject(r.digest));
          outputs.append(b);
        }
        $("detail-content").append(outputs);
      }
    } catch {}
  } catch (e) {
    if (version === detailVersion)
      replace("detail-content", el("p", e.message, "detail-failure"));
  }
}
async function openObject(digest) {
  const version = openPanel("IMMUTABLE OUTPUT", "Object preview");
  try {
    const o = await api("/api/object?" + new URLSearchParams({ digest }));
    if (version !== detailVersion) return;
    replace(
      "detail-content",
      detailLine("SHA-256", digest),
      preview(
        o.binary ? "Binary data · hex preview" : "Object content",
        o.preview,
      ),
    );
    const back = el("button", "Copy digest", "button");
    back.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(digest);
        back.textContent = "Copied";
      } catch {
        back.textContent = "Select the digest above to copy";
      }
    });
    $("detail-content").append(back);
  } catch (e) {
    if (version === detailVersion)
      replace("detail-content", el("p", e.message, "detail-failure"));
  }
}
function openNode(n, now) {
  openPanel("MEMBERSHIP", "Node details");
  const s = section(n.session.node);
  s.append(
    badge(n.status),
    detailLine("Node", n.session.node),
    detailLine("Incarnation", n.session.incarnation),
    detailLine("Lease expires", date(n.lease_until)),
    detailLine("Snapshot time", date(now)),
    detailLine("Active jobs", n.running),
    el(
      "p",
      n.status === "expired"
        ? "The node lease expired. The process may be stopped or disconnected; this does not prove the machine is down."
        : n.status === "draining"
          ? "This worker is finishing claimed work and is not accepting new claims."
          : "The node has a live registration. Active jobs require a live work lease as well.",
      "detail-note",
    ),
  );
  replace("detail-content", s);
}
for (const b of document.querySelectorAll("[data-resource]"))
  b.addEventListener("click", () =>
    choose(b.dataset.resource, b.dataset.phase || ""),
  );
// Pending is a combined metric; open the full task list rather than only Waiting.
document.querySelector('[data-phase="waiting"]').removeAttribute("data-phase");
$("refresh").addEventListener("click", () => refresh(true));
$("phase").addEventListener("change", () => {
  view.phase = $("phase").value;
  view.offset = 0;
  refresh(true);
});
let debounce;
$("search").addEventListener("input", () => {
  clearTimeout(debounce);
  debounce = setTimeout(() => {
    view.search = $("search").value;
    view.offset = 0;
    refresh(true);
  }, 250);
});
$("previous").addEventListener("click", () => {
  view.offset = Math.max(0, view.offset - view.limit);
  refresh(true);
});
$("next").addEventListener("click", () => {
  view.offset += view.limit;
  refresh(true);
});
$("close-detail").addEventListener("click", () => $("detail").close());
$("detail").addEventListener("close", () => {
  detailVersion++;
});
$("detail").addEventListener("click", (e) => {
  if (
    e.target === $("detail") &&
    e.clientX < $("detail").getBoundingClientRect().left
  )
    $("detail").close();
});
$("auto").addEventListener("change", () => {
  if ($("auto").checked) refresh(true);
  else if (!$("connection").classList.contains("stale"))
    $("connection-text").textContent = "Auto-refresh paused";
});
document.addEventListener("keydown", (e) => {
  if (
    e.key === "/" &&
    !["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName) &&
    !$("detail").open
  ) {
    e.preventDefault();
    $("search").focus();
  }
});
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && $("auto").checked) refresh();
});
async function start() {
  try {
    const config = await api("/api/config");
    $("namespace").textContent = config.namespace;
    $("namespace").title = config.namespace;
    document.title = `Loom · ${config.namespace}`;
  } catch {
    $("namespace").textContent = "Unavailable";
  }
  await refresh();
  setInterval(() => {
    if ($("auto").checked && !document.hidden) refresh();
  }, 3000);
}
start();
