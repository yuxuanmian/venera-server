(() => {
  const $ = (id) => document.getElementById(id);
  let status = null;
  const text = (node, value) => { node.textContent = value == null ? '' : String(value); };
  const pointer = (item) => item ? `${item.catalogId}@${item.revision}` : '暂无';
  const setBusy = (busy) => {
    $('check').disabled = busy;
    $('activate').disabled = busy || !status || !status.candidate;
  };
  const showError = (message) => text($('error'), message || '');
  async function request(path, options) {
    const response = await fetch(path, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.error ? payload.error.message : `HTTP ${response.status}`);
    return payload;
  }
  function render() {
    if (!status) return;
    const source = status.configuredSource || {};
    $('configured').replaceChildren();
    [['地址', source.catalogUrl], ['目录', source.catalogId], ['ref', source.ref]].forEach(([label, value]) => {
      const dt = document.createElement('dt'); text(dt, label);
      const dd = document.createElement('dd'); text(dd, value); $('configured').append(dt, dd);
    });
    text($('active'), status.active ? `${pointer(status.active)}（${status.active.sourceCount} 个源）` : '未激活');
    text($('candidate'), status.candidate ? `${pointer(status.candidate)}（${status.candidate.sourceCount} 个源）` : '暂无候选');
    $('activate').disabled = status.busy || !status.candidate;
    const history = $('history'); history.replaceChildren();
    (status.history || []).forEach((item) => {
      const li = document.createElement('li');
      const button = document.createElement('button'); text(button, `激活 ${pointer(item)}`);
      button.addEventListener('click', () => activate(item)); li.append(pointer(item), ' ', button); history.append(li);
    });
  }
  async function load() {
    try { status = await request('/admin/api/catalog/status'); showError(''); render(); }
    catch (error) { showError(error.message); }
  }
  async function check() {
    setBusy(true); showError('');
    try { status = await request('/admin/api/catalog/check', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' }); await load(); }
    catch (error) { showError(error.message); setBusy(false); }
  }
  async function activate(item) {
    setBusy(true); showError('');
    try { await request('/admin/api/catalog/activate', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({catalogId: item.catalogId, revision: item.revision}) }); await load(); }
    catch (error) { showError(error.message); setBusy(false); }
  }
  $('check').addEventListener('click', check);
  $('activate').addEventListener('click', () => activate(status.candidate));
  load();
})();
