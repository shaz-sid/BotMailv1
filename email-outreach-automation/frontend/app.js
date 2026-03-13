const API = "http://localhost:8000/api/v1";

/* ─── Utility: Central API Fetch ─────────────────────────── */
async function apiFetch(path, options = {}) {
  const res = await fetch(API + path, options);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

/* ─── Utility: Toast Notifications ──────────────────────── */
function showToast(message, type = "info") {
  const toast = document.getElementById("toast");
  toast.textContent = message;
  toast.className = `toast ${type}`;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => toast.classList.add("hidden"), 3500);
}

/* ─── Utility: Loading State ─────────────────────────────── */
function setLoading(id, visible) {
  document.getElementById(id)?.classList.toggle("hidden", !visible);
}

/* ─── Utility: Button Spinner ────────────────────────────── */
function setBtnState(btn, loading, originalText) {
  btn.disabled = loading;
  btn.textContent = loading ? "Please wait…" : originalText;
}

/* ─── Sidebar Navigation ─────────────────────────────────── */
document.querySelectorAll(".nav-link").forEach(link => {
  link.addEventListener("click", e => {
    e.preventDefault();
    const target = link.dataset.section;
    document.querySelectorAll(".nav-link").forEach(l => l.classList.remove("active"));
    document.querySelectorAll(".page").forEach(p => p.classList.add("hidden"));
    link.classList.add("active");
    document.getElementById(`section-${target}`)?.classList.remove("hidden");
  });
});

/* ─── Upload Contacts ────────────────────────────────────── */
document.getElementById("btnUpload").addEventListener("click", async () => {
  const fileInput = document.getElementById("csvFile");
  const resultBox = document.getElementById("uploadResult");
  const btn = document.getElementById("btnUpload");

  if (!fileInput.files.length) {
    showToast("Please select a CSV file first.", "error");
    return;
  }

  const form = new FormData();
  form.append("file", fileInput.files[0]);
  setBtnState(btn, true, "Upload Contacts");

  try {
    const data = await apiFetch("/contacts/upload", { method: "POST", body: form });
    resultBox.textContent = JSON.stringify(data, null, 2);
    resultBox.classList.remove("hidden");
    showToast(`✅ Uploaded successfully!`, "success");
    loadContacts();
  } catch (err) {
    showToast(`Upload failed: ${err.message}`, "error");
  } finally {
    setBtnState(btn, false, "Upload Contacts");
  }
});

/* ─── Load Contacts ──────────────────────────────────────── */
async function loadContacts() {
  const tbody = document.getElementById("contactsTbody");
  setLoading("contactsLoading", true);
  try {
    const data = await apiFetch("/contacts");
    tbody.innerHTML = "";
    if (!data.contacts?.length) {
      tbody.innerHTML = `<tr><td colspan="4" class="empty-state">No contacts found.</td></tr>`;
      return;
    }
    data.contacts.forEach(c => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${c.id}</td>
        <td>${escapeHtml(c.name ?? "—")}</td>
        <td>${escapeHtml(c.email)}</td>
        <td>${escapeHtml(c.company ?? "—")}</td>
      `;
      tbody.appendChild(row);
    });
  } catch (err) {
    showToast(`Failed to load contacts: ${err.message}`, "error");
  } finally {
    setLoading("contactsLoading", false);
  }
}
document.getElementById("btnLoadContacts").addEventListener("click", loadContacts);

/* ─── Create Campaign ────────────────────────────────────── */
document.getElementById("btnCreateCampaign").addEventListener("click", async () => {
  const name = document.getElementById("campaignName").value.trim();
  const description = document.getElementById("campaignDescription").value.trim();
  const btn = document.getElementById("btnCreateCampaign");

  if (!name) {
    showToast("Campaign name is required.", "error");
    return;
  }

  setBtnState(btn, true, "➕ Create Campaign");
  try {
    const data = await apiFetch("/campaigns", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, description }),
    });
    showToast(`Campaign "${data.name ?? name}" created (ID: ${data.id})`, "success");
    document.getElementById("campaignName").value = "";
    document.getElementById("campaignDescription").value = "";
    loadCampaigns();
  } catch (err) {
    showToast(`Create failed: ${err.message}`, "error");
  } finally {
    setBtnState(btn, false, "➕ Create Campaign");
  }
});

/* ─── Load Campaigns ─────────────────────────────────────── */
async function loadCampaigns() {
  const tbody = document.getElementById("campaignTbody");
  setLoading("campaignsLoading", true);
  try {
    const data = await apiFetch("/campaigns");
    tbody.innerHTML = "";
    if (!data.campaigns?.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="empty-state">No campaigns yet.</td></tr>`;
      return;
    }
    data.campaigns.forEach(c => {
      const statusClass = {
        draft: "badge-draft", running: "badge-running",
        completed: "badge-completed", failed: "badge-failed"
      }[c.status] ?? "badge-draft";

      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${c.id}</td>
        <td>${escapeHtml(c.name)}</td>
        <td><span class="badge ${statusClass}">${c.status}</span></td>
        <td>${c.total_sent ?? 0}</td>
        <td>
          <button class="btn btn-success btn-sm" data-id="${c.id}" data-action="run">▶ Run</button>
          <button class="btn btn-secondary btn-sm" data-id="${c.id}" data-action="dry-run" style="margin-left:6px">🔍 Dry Run</button>
        </td>
      `;
      tbody.appendChild(row);
    });
  } catch (err) {
    showToast(`Failed to load campaigns: ${err.message}`, "error");
  } finally {
    setLoading("campaignsLoading", false);
  }
}
document.getElementById("btnLoadCampaigns").addEventListener("click", loadCampaigns);

/* ─── Run Campaign (delegated) ───────────────────────────── */
document.getElementById("campaignTbody").addEventListener("click", async e => {
  const btn = e.target.closest("[data-action]");
  if (!btn) return;
  const id = btn.dataset.id;
  const isDryRun = btn.dataset.action === "dry-run";
  const originalText = btn.textContent;
  setBtnState(btn, true, originalText);
  try {
    await apiFetch(`/campaigns/${id}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dry_run: isDryRun }),
    });
    showToast(isDryRun ? `Dry run started for campaign ${id}` : `Campaign ${id} queued!`, "success");
    setTimeout(loadCampaigns, 800);
  } catch (err) {
    showToast(`Run failed: ${err.message}`, "error");
  } finally {
    setBtnState(btn, false, originalText);
  }
});

/* ─── Preview Email ──────────────────────────────────────── */
document.getElementById("btnPreview").addEventListener("click", async () => {
  const campaignId = document.getElementById("previewCampaignId").value.trim();
  const contactId  = document.getElementById("previewContactId").value.trim();
  const result     = document.getElementById("previewResult");
  const btn        = document.getElementById("btnPreview");

  if (!campaignId || !contactId) {
    showToast("Both Campaign ID and Contact ID are required.", "error");
    return;
  }

  setBtnState(btn, true, "👁 Preview Email");
  try {
    const data = await apiFetch(
      `/campaigns/${campaignId}/preview?contact_id=${contactId}`,
      { method: "POST" }
    );
    result.innerHTML = `<strong>Subject:</strong> ${escapeHtml(data.subject)}\n\n${escapeHtml(data.body)}`;
    result.classList.remove("hidden");
  } catch (err) {
    showToast(`Preview failed: ${err.message}`, "error");
  } finally {
    setBtnState(btn, false, "👁 Preview Email");
  }
});

/* ─── Load Stats ─────────────────────────────────────────── */
document.getElementById("btnLoadStats").addEventListener("click", async () => {
  const id     = document.getElementById("statsCampaignId").value.trim();
  const wrap   = document.getElementById("statsResult");
  const grid   = document.getElementById("statsGrid");
  const btn    = document.getElementById("btnLoadStats");

  if (!id) {
    showToast("Please enter a Campaign ID.", "error");
    return;
  }

  setBtnState(btn, true, "📊 Load Stats");
  try {
    const data = await apiFetch(`/campaigns/${id}/stats`);
    grid.innerHTML = "";

    const statKeys = {
      total_sent:    "Total Sent",
      total_opened:  "Opened",
      total_clicked: "Clicked",
      total_failed:  "Failed",
      open_rate:     "Open Rate",
      click_rate:    "Click Rate",
    };

    Object.entries(statKeys).forEach(([key, label]) => {
      if (data[key] !== undefined) {
        const card = document.createElement("div");
        card.className = "stat-card";
        const val = typeof data[key] === "number" && data[key] < 2
          ? `${(data[key] * 100).toFixed(1)}%`
          : data[key];
        card.innerHTML = `<div class="stat-value">${val}</div><div class="stat-label">${label}</div>`;
        grid.appendChild(card);
      }
    });

    wrap.classList.remove("hidden");
  } catch (err) {
    showToast(`Stats failed: ${err.message}`, "error");
  } finally {
    setBtnState(btn, false, "📊 Load Stats");
  }
});

/* ─── XSS Guard ──────────────────────────────────────────── */
function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
