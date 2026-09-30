// ===========================================================================
// Panel del server
// Quien esta conectado, que mensajes circulan, envio por nombre o por URL
// libre y log de eventos en vivo.
// ===========================================================================

const ADMIN_KEY = "admin" // debe coincidir con ADMIN_KEY del server

const el = id => document.getElementById(id)

async function api(path, options = {}) {
  let res
  try {
    res = await fetch(path, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        "x-admin-key": ADMIN_KEY,
        "ngrok-skip-browser-warning": "true",
        ...(options.headers || {})
      }
    })
  } catch {
    throw new Error("Sin conexión con tu server. ¿Sigue corriendo 'node server.js'?")
  }

  // Se lee como texto: si lo que llega no es JSON, quien contestó NO fue tu
  // server sino algo que hay delante (ngrok, un proxy), y hay que decirlo.
  // Tragarse el cuerpo dejaba errores mudos tipo 'HTTP 502'.
  const cuerpo = await res.text()
  let data = {}

  try {
    data = cuerpo ? JSON.parse(cuerpo) : {}
  } catch {
    const ngrok = cuerpo.match(/ERR_NGROK_[0-9]+/)
    const detalle = ngrok
      ? `Contestó el túnel, no tu server (${ngrok[0]}). Revisa que 'node server.js' siga corriendo y que ngrok apunte a su puerto.`
      : `El panel recibió un ${res.status} en HTML, no JSON: contestó algo delante de tu server (ngrok o un proxy), no el server.`
    throw Object.assign(new Error(detalle), { data: { detail: detalle } })
  }

  if (!res.ok) throw Object.assign(new Error(data.error || `HTTP ${res.status}`), { data })
  return data
}

const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]))
const fmtTime = ts => new Date(ts).toLocaleTimeString()

function fmtDur(ms) {
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${s % 60}s`
  const h = Math.floor(m / 60)
  return `${h}h ${m % 60}m`
}

// ------------------------------------------------------------------- toasts

function toast(kind, title, detail = "") {
  const icons = { ok: "✓", bad: "✕", info: "•" }
  const node = document.createElement("div")
  node.className = `toast ${kind}`
  node.innerHTML = `<span class="toast-icon">${icons[kind] || "•"}</span>
    <div><b>${esc(title)}</b>${detail ? `<small>${esc(detail)}</small>` : ""}</div>`
  el("toasts").appendChild(node)

  // Los errores traen explicacion larga: se quedan mas tiempo para poder leerlos
  setTimeout(() => {
    node.classList.add("out")
    setTimeout(() => node.remove(), 220)
  }, kind === "bad" ? 11000 : 4200)
}

// -------------------------------------------------------------------- tema

const savedTheme = (() => {
  try { return localStorage.getItem("mw-theme") } catch { return null }
})()

document.documentElement.dataset.theme =
  savedTheme || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light")

el("theme-toggle").addEventListener("click", () => {
  const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark"
  document.documentElement.dataset.theme = next
  try { localStorage.setItem("mw-theme", next) } catch { /* modo privado */ }
})

// ------------------------------------------------------------------ estado

let servers = []
let messages = []

function renderHeader(status) {
  const mw = status.server
  el("mw-info").textContent =
    `${mw.publicUrl || `http://localhost:${mw.port}`}${mw.tunnel ? " (túnel)" : ""}` +
    ` · ${mw.host.hostname} (${mw.host.platform}/${mw.host.arch})` +
    ` · activo ${fmtDur(mw.uptimeMs)} · timeout ${mw.timeout / 1000}s`

  // Eleccion de lider: que papel tiene este server y quien manda
  const e = status.election || {}
  const link = el("election-link")
  if (!e.enabled) {
    link.hidden = true
  } else {
    const papel = e.paused ? "pausado" : e.role === "leader" ? "líder" : e.role === "candidate" ? "en elección" : "seguidor"
    link.textContent = `${e.id} · ${papel}${e.role !== "leader" && e.leader ? ` · manda ${e.leader}` : ""}`
    link.className = `pill ${e.paused ? "pill-dead" : e.role === "leader" ? "pill-alive" : "pill-idle"}`
  }

  const c = status.counts
  el("kpis").innerHTML = [
    ["i-ok", "●", c.alive, "en línea"],
    ["i-warn", "▲", c.suspect, "sin pulso"],
    ["i-bad", "✕", c.dead, "caídos"],
    ["i-info", "✉", c.messages, "mensajes"]
  ].map(([cls, icon, value, label]) => `
    <div class="kpi">
      <div class="kpi-icon ${cls}">${icon}</div>
      <div class="kpi-body"><b>${value}</b><span>${label}</span></div>
    </div>`).join("")
}

// ------------------------------------------------------------- conectados

function renderConnected(list) {
  const ul = el("connected")
  el("node-count").textContent = list.length

  if (!list.length) {
    ul.innerHTML = `<li class="empty">Nadie conectado. Arranca un nodo con
      <code>node miniserver.js 4000 alfa</code> y aparecerá aquí.</li>`
    return
  }

  ul.innerHTML = list.map(s => {
    const key = s.status.toLowerCase()
    const label = { alive: "en línea", suspect: "sin pulso", dead: "caído" }[key] || s.status
    const probe = s.reachable === true ? `${s.latencyMs} ms`
      : s.reachable === false ? "sin respuesta" : "—"

    // Últimos mensajes que ha enviado este nodo
    const sent = messages.filter(m => m.from === s.name).slice(-3).reverse()
    const activity = sent.length
      ? sent.map(m => `<div class="sent-line">
            <span class="sent-to">→ ${esc(m.to || "server")}</span>
            <span class="sent-msg">${esc(m.message)}</span>
            <span class="sent-ts">${fmtTime(m.timestamp)}</span>
          </div>`).join("")
      : `<div class="sent-line sent-none">todavía no ha enviado mensajes</div>`

    return `<li class="conn is-${key}">
      <div class="conn-main">
        <div class="conn-id">
          <span class="conn-name">${esc(s.name)}</span>
          <span class="pill pill-${key}"><i class="dot"></i>${label}</span>
        </div>
        <div class="conn-url">${esc(s.url)}</div>
        <div class="conn-meta">
          ${s.host ? `${esc(s.host.hostname)} · ${esc(s.host.platform)}/${esc(s.host.arch)} · ` : ""}
          ${s.pulses} pulsos · último hace ${s.secondsSincePulse}s · sonda ${probe} · conectado ${fmtDur(s.uptimeMs)}
        </div>
      </div>
      <div class="conn-activity">
        <div class="conn-activity-head">${s.messages} mensaje(s) enviado(s)</div>
        ${activity}
      </div>
    </li>`
  }).join("")
}

// ---------------------------------------------------------------- personas


// ---------------------------------------------------------------- mensajes

let msgFilter = ""

function renderMessageFilters() {
  const box = document.querySelector(".mfilter").parentElement
  const senders = [...new Set(messages.map(m => m.from))]

  box.innerHTML = `<button class="mfilter chip ${msgFilter ? "" : "active"}" data-from="">Todos</button>` +
    senders.map(s => `<button class="mfilter chip ${msgFilter === s ? "active" : ""}" data-from="${esc(s)}">${esc(s)}</button>`).join("")
}

function renderMessages() {
  const ul = el("messages-list")
  const list = (msgFilter ? messages.filter(m => m.from === msgFilter) : messages).slice(-60)

  if (!list.length) {
    ul.innerHTML = `<li class="empty">Todavía no circulan mensajes</li>`
    return
  }

  ul.innerHTML = list.slice().reverse().map(m => {
    const state = m.delivered === false
      ? `<span class="tick bad" title="${esc(m.error || "")}">no entregado</span>`
      : m.delivered === true ? `<span class="tick ok">entregado</span>` : ""
    const kind = m.broadcast ? `<span class="tick info">broadcast</span>`
      : m.direct ? `<span class="tick info">URL directa</span>` : ""

    return `<li class="msg">
      <div class="msg-head">
        <span class="who from">${esc(m.from)}</span>
        <span class="arrow">→</span>
        <span class="who to">${esc(m.to || "server")}</span>
        ${kind}${state}
        <span class="msg-ts">${fmtTime(m.timestamp)}</span>
      </div>
      <div class="msg-body">${esc(m.message)}</div>
    </li>`
  }).join("")
}

document.querySelector(".mfilter").parentElement.addEventListener("click", ev => {
  const chip = ev.target.closest(".mfilter")
  if (!chip) return
  msgFilter = chip.dataset.from
  renderMessageFilters()
  renderMessages()
})

// ------------------------------------------------------------- envío de msg

// El nombre de quien envía se recuerda: es siempre el mismo y no tiene
// sentido reescribirlo en cada mensaje.
try {
  const guardado = localStorage.getItem("mw-name")
  if (guardado) el("msg-from").value = guardado
} catch { /* modo privado */ }

el("send-form").addEventListener("submit", async ev => {
  ev.preventDefault()

  const url = el("msg-url").value.trim()
  const name = el("msg-from").value.trim()
  const message = el("msg-text").value.trim()

  if (!url) return toast("bad", "Falta la URL de destino")
  if (!name) return toast("bad", "Falta 'name'", "es quién envía el mensaje")
  if (!message) return toast("bad", "Falta 'message'")

  try { localStorage.setItem("mw-name", name) } catch { /* modo privado */ }

  const boton = el("send-form").querySelector("button[type=submit]")
  boton.disabled = true
  boton.textContent = "Enviando…"

  try {
    // El server hace la entrega por nosotros: así no choca con CORS y el
    // mensaje queda en el historial del panel.
    const res = await api("/send-to-url", {
      method: "POST",
      body: JSON.stringify({ url, name, message })
    })
    toast("ok", `Enviado como '${name}'`, res.to || url)
    el("msg-text").value = ""
    el("msg-text").focus()
  } catch (e) {
    // 'probado' solo llega cuando el destino contestó pero ninguna ruta existía;
    // si el fallo fue del túnel o de la conexión, listar rutas despista.
    const probado = e.data?.probado?.length ? ` · rutas probadas: ${e.data.probado.join(", ")}` : ""
    toast("bad", "No se pudo enviar", (e.data?.detail || e.message) + probado)
  } finally {
    boton.disabled = false
    boton.textContent = "Enviar"
  }

  refresh()
})

// Comprobar qué hay en la URL antes de escribir. Sirve para saber si el
// problema es del destino (su server apagado, su ngrok mal apuntado) y no
// quedarse adivinando delante de un error.
el("probe-btn").addEventListener("click", async () => {
  const url = el("msg-url").value.trim()
  if (!url) return toast("bad", "Escribe primero la URL")

  const boton = el("probe-btn")
  boton.disabled = true
  boton.textContent = "Probando…"

  try {
    const r = await api("/probe-url", { method: "POST", body: JSON.stringify({ url }) })
    toast(r.vivo ? "ok" : "bad", r.estado, r.detalle)
  } catch (e) {
    toast("bad", "No se pudo comprobar", e.data?.detail || e.message)
  } finally {
    boton.disabled = false
    boton.textContent = "Probar"
  }
})

// ------------------------------------------------------------- identidad

let identityConfigured = false

async function loadIdentity() {
  try {
    const info = await api("/node/identity")
    identityConfigured = Boolean(info.id)

    const button = el("me")
    if (button) {
      button.textContent = info.id ? `ID: ${info.id}` : "Configurar ID"
      button.className = info.id ? "pill pill-live" : "pill pill-idle"
    }

    const modal = el("identity-modal")
    if (modal && !info.id) {
      modal.hidden = false
      modal.style.display = "flex"
      setTimeout(() => el("identity-id")?.focus(), 50)
    }
  } catch (e) {
    console.error("No se pudo consultar la identidad:", e)
  }
}

async function saveIdentity(event) {
  event.preventDefault()
  const input = el("identity-id")
  const msg = el("identity-msg")
  const id = input.value.trim()
  msg.textContent = "Guardando…"

  try {
    const result = await api("/node/identity", {
      method: "POST",
      body: JSON.stringify({ id })
    })

    if (!result.ok) {
      msg.textContent = result.error || "No se pudo guardar el ID"
      return
    }

    identityConfigured = true
    const modal = el("identity-modal")
    modal.hidden = true
    modal.style.display = "none"

    const button = el("me")
    if (button) {
      button.textContent = `ID: ${result.id}`
      button.className = "pill pill-live"
    }

    refresh()
  } catch (e) {
    msg.textContent = e.message || "No se pudo guardar el ID"
  }
}

el("identity-form")?.addEventListener("submit", saveIdentity)

// ------------------------------------------------------------ tiempo real

function connectStream() {
  const source = new EventSource("/events")
  const badge = el("stream-state")

  source.onopen = () => {
    badge.className = "pill pill-live"
    badge.innerHTML = `<i class="dot"></i>en vivo`
  }

  source.onmessage = ev => {
    let payload
    try { payload = JSON.parse(ev.data) } catch { return }
    if (payload.kind !== "log") return

    refresh()
  }

  source.onerror = () => {
    badge.className = "pill pill-idle"
    badge.innerHTML = `<i class="dot"></i>reconectando`
  }
}

// ---------------------------------------------------------------- refresco

let refreshing = false

async function refresh() {
  if (refreshing) return
  refreshing = true
  try {
    const [status, msgs] = await Promise.all([api("/api/status"), api("/messages")])
    servers = status.servers
    messages = msgs
    renderHeader(status)
    renderConnected(servers)
    renderMessageFilters()
    renderMessages()
  } catch (e) {
    console.error("Error al refrescar:", e)
  } finally {
    refreshing = false
  }
}

async function init() {
  await loadIdentity()
  await refresh()
  connectStream()
  setInterval(refresh, 3000)
}

init()
