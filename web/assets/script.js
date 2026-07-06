/**
 * Keplr Delegation Script for Tellor Layer
 * Uses Direct signing (Protobuf) for compatibility with custom message types
 */

// Simple Long polyfill for Keplr signDirect
const Long = {
  fromString: function(str) {
    const num = parseInt(str, 10);
    return {
      low: num | 0,
      high: Math.floor(num / 0x100000000) | 0,
      unsigned: false,
      toNumber: function() { return num; },
      toString: function() { return str; }
    };
  },
  fromNumber: function(num) {
    return {
      low: num | 0,
      high: Math.floor(num / 0x100000000) | 0,
      unsigned: false,
      toNumber: function() { return num; },
      toString: function() { return String(num); }
    };
  }
};

// Async tree loading - polls /api/tree if tree is in loading state
(function() {
  const treeLoading = document.getElementById('treeLoading');
  const treeContainer = document.getElementById('treeContainer');
  const cacheTimeEl = document.getElementById('cacheTime');

  if (!treeContainer) return;

  // Poll for tree data
  let pollCount = 0;
  const maxPolls = 30; // Max 60 seconds (30 * 2s)

  async function pollTree() {
    try {
      const period = new URLSearchParams(window.location.search).get('period') || '';
      const url = period ? `/api/tree?period=${period}` : '/api/tree';
      const response = await fetch(url);
      if (!response.ok) throw new Error('Failed to fetch tree');

      const data = await response.json();

      if (data.loading && pollCount < maxPolls) {
        // Still loading, poll again
        pollCount++;
        setTimeout(pollTree, 2000);
        return;
      }

      if (data.validators && data.validators.length > 0) {
        // Render tree
        renderTree(data.validators);
        if (cacheTimeEl && data.timestamp) {
          cacheTimeEl.textContent = data.timestamp;
        }
      } else if (!data.loading) {
        // No data and not loading
        treeContainer.innerHTML = '<div class="tree-empty">No validator data available</div>';
      }
    } catch (error) {
      console.error('Tree poll failed:', error);
      if (pollCount < maxPolls) {
        pollCount++;
        setTimeout(pollTree, 2000);
      }
    }
  }

  function renderTree(validators) {
    // Hide loading indicator if it exists
    if (treeLoading) {
      treeLoading.style.display = 'none';
    }

    let html = '<div class="tree-hierarchy">';

    for (const v of validators) {
      const moniker = v.moniker || '';
      const shortAddr = v.short_address || '';
      const displayName = moniker ? `${moniker}: ${shortAddr}` : shortAddr;
      const missedBlocks = v.missed_blocks || 0;
      const missedBlocksPct = v.missed_blocks_pct || '0%';
      const hasReporters = v.has_reporters && v.reporters && v.reporters.length > 0;
      const status = v.status || 'Active';
      const statusClass = status === 'Jailed' ? 'status-jailed' : (status === 'Degraded' ? 'status-warning' : 'status-active');
      const statusText = status;
      const rewards = v.rewards || '-';

      // Validator Card
      html += `
        <div class="tree-validator-group">
          <div class="tree-validator-card">
            <div class="tree-validator-header">
              <div class="tree-validator-info">
                <div class="tree-icon tree-icon-validator">
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <rect x="3" y="3" width="7" height="7" rx="1" stroke="currentColor" stroke-width="2"/>
                    <rect x="14" y="3" width="7" height="7" rx="1" stroke="currentColor" stroke-width="2"/>
                    <rect x="14" y="14" width="7" height="7" rx="1" stroke="currentColor" stroke-width="2"/>
                    <rect x="3" y="14" width="7" height="7" rx="1" stroke="currentColor" stroke-width="2"/>
                  </svg>
                </div>
                <div>
                  <h3 class="tree-validator-name">${escapeHtml(displayName)}</h3>
                  <div class="tree-validator-status">
                    <span class="tree-status-dot ${statusClass}"></span>
                    <span class="tree-status-text ${statusClass}">${statusText}</span>
                  </div>
                </div>
              </div>
              <div class="tree-validator-stats">
                <div class="tree-stat">
                  <div class="tree-stat-label">Commission</div>
                  <div class="tree-stat-value">${escapeHtml(v.commission)}</div>
                </div>
                <div class="tree-stat">
                  <div class="tree-stat-label">Staked (TRB)</div>
                  <div class="tree-stat-value">${escapeHtml(v.power_trb)}</div>
                </div>
                <div class="tree-stat">
                  <div class="tree-stat-label">Rewards</div>
                  <div class="tree-stat-value tree-stat-rewards">${escapeHtml(rewards)}</div>
                </div>
                <div class="tree-stat">
                  <div class="tree-stat-label">Missed</div>
                  <div class="tree-stat-value ${missedBlocks > 0 ? 'tree-stat-warning' : ''}" title="${missedBlocks} missed blocks">${escapeHtml(missedBlocksPct)}</div>
                </div>
              </div>
            </div>
          </div>`;

      // Reporters
      if (hasReporters) {
        html += '<div class="tree-reporters-container">';

        for (let rIdx = 0; rIdx < v.reporters.length; rIdx++) {
          const r = v.reporters[rIdx];
          const rMoniker = r.moniker || '';
          const rShortAddr = r.short_address || '';
          const rDisplayName = rMoniker ? `${rMoniker}: ${rShortAddr}` : rShortAddr;
          const missedCycles = r.missed_cycles || 0;
          const missedCyclesPct = r.missed_cycles_pct || '0%';
          const rStatus = r.status || 'Active';
          const rStatusClass = rStatus === 'Jailed' ? 'status-jailed' : (rStatus === 'Degraded' ? 'status-warning' : 'status-active');
          const rStake = r.is_self_selector && r.self_stake ? r.self_stake : '-';
          const rRewards = r.rewards || '-';
          const hasSelectors = r.selectors && r.selectors.length > 0;
          const isLastReporter = rIdx === v.reporters.length - 1;

          html += `
            <div class="tree-reporter-group ${isLastReporter ? 'tree-last' : ''}">
              <div class="tree-line-horizontal"></div>
              <div class="tree-reporter-card">
                <div class="tree-reporter-header">
                  <div class="tree-reporter-info">
                    <div class="tree-icon tree-icon-reporter">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M12 2L2 7V17L12 22L22 17V7L12 2Z" stroke="currentColor" stroke-width="2"/>
                      </svg>
                    </div>
                    <span class="tree-reporter-name">${escapeHtml(rDisplayName)}</span>
                    <span class="tree-status-dot ${rStatusClass}" title="${rStatus}"></span>
                  </div>
                  <div class="tree-reporter-stats">
                    <span class="tree-stat-value-inline">${escapeHtml(r.commission)}</span>
                    <span class="tree-stat-value-inline">${escapeHtml(rStake)}</span>
                    <span class="tree-stat-value-inline tree-stat-rewards">${escapeHtml(rRewards)}</span>
                    <span class="tree-stat-value-inline ${missedCycles > 0 ? 'tree-stat-warning' : ''}" title="${missedCycles} missed reports">${escapeHtml(missedCyclesPct)}</span>
                  </div>
                </div>
              </div>`;

          // Selectors
          if (hasSelectors) {
            html += '<div class="tree-selectors-container">';
            for (let sIdx = 0; sIdx < r.selectors.length; sIdx++) {
              const s = r.selectors[sIdx];
              const sMoniker = s.moniker || '';
              const sShortAddr = s.short_address || '';
              const sDisplayName = sMoniker ? `${sMoniker}: ${sShortAddr}` : sShortAddr;
              const sRewards = s.rewards || '-';
              const isLastSelector = sIdx === r.selectors.length - 1;

              html += `
                <div class="tree-selector-group ${isLastSelector ? 'tree-last' : ''}">
                  <div class="tree-line-horizontal"></div>
                  <div class="tree-selector-card">
                    <span class="tree-selector-dot"></span>
                    <span class="tree-selector-name">${escapeHtml(sDisplayName)}</span>
                    <div class="tree-selector-stats">
                      <span class="tree-stat-value-inline">-</span>
                      <span class="tree-stat-value-inline">${escapeHtml(s.stake)}</span>
                      <span class="tree-stat-value-inline tree-stat-rewards">${escapeHtml(sRewards)}</span>
                      <span class="tree-stat-value-inline">-</span>
                    </div>
                  </div>
                </div>`;
            }
            html += '</div>';
          }

          html += '</div>';
        }

        html += '</div>';
      } else {
        html += `
          <div class="tree-reporters-container tree-empty-reporters">
            <div class="tree-line-horizontal tree-line-dashed"></div>
            <span class="tree-empty-text">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="display: inline; vertical-align: middle; margin-right: 6px;">
                <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
                <path d="M12 8v4M12 16h.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
              </svg>
              No reporters assigned
            </span>
          </div>`;
      }

      // Delegators to this validator, independent of reporter selection. Shows any stake
      // delegated to us even when the delegator selected a different reporter (or none).
      if (v.has_delegators && v.delegators && v.delegators.length > 0) {
        html += `
          <div class="tree-reporters-container">
            <div class="tree-reporter-group">
              <div class="tree-line-horizontal"></div>
              <div class="tree-reporter-card">
                <div class="tree-reporter-header">
                  <div class="tree-reporter-info">
                    <span class="tree-reporter-name">Delegators to this validator</span>
                  </div>
                </div>
              </div>
              <div class="tree-selectors-container">`;
        for (let dIdx = 0; dIdx < v.delegators.length; dIdx++) {
          const d = v.delegators[dIdx];
          const isLastDel = dIdx === v.delegators.length - 1;
          html += `
                <div class="tree-selector-group ${isLastDel ? 'tree-last' : ''}">
                  <div class="tree-line-horizontal"></div>
                  <div class="tree-selector-card">
                    <span class="tree-selector-dot"></span>
                    <span class="tree-selector-name">${escapeHtml(d.short_address)}</span>
                    <div class="tree-selector-stats">
                      <span class="tree-stat-value-inline">${escapeHtml(d.stake)}</span>
                      <span class="tree-stat-value-inline" title="reporter selected">${escapeHtml(d.reporter)}</span>
                    </div>
                  </div>
                </div>`;
        }
        html += `
              </div>
            </div>
          </div>`;
      }

      html += '</div>';
    }

    html += '</div>';
    treeContainer.innerHTML = html;
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // Start polling
  pollTree();
})();


// Refresh the validator tree cache
async function refreshTree() {
  const btn = document.getElementById('refreshBtn');
  const cacheTimeEl = document.getElementById('cacheTime');

  if (!btn) return;

  btn.disabled = true;
  btn.classList.add('loading');

  try {
    const response = await fetch('/api/refresh-tree', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });

    if (!response.ok) {
      throw new Error('Failed to refresh tree');
    }

    const data = await response.json();

    if (data.success && cacheTimeEl) {
      cacheTimeEl.textContent = data.timestamp;
    }

    // Reload the page to show updated data
    window.location.reload();
  } catch (error) {
    console.error('Refresh failed:', error);
    alert('Failed to refresh tree. Please try again.');
  } finally {
    btn.disabled = false;
    btn.classList.remove('loading');
  }
}

// Format number with thousand separators
function formatNumber(value) {
  const parts = value.split('.');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return parts.join('.');
}

// Parse formatted number back to float
function parseFormattedNumber(value) {
  return parseFloat(value.replace(/,/g, ''));
}

// Handle amount input formatting
function handleAmountInput(e) {
  const input = e.target;
  let value = input.value.replace(/[^\d.,]/g, '');
  value = value.replace(/,/g, '');

  const parts = value.split('.');
  if (parts.length > 2) {
    value = parts[0] + '.' + parts.slice(1).join('');
  }

  if (value) {
    const numParts = value.split('.');
    numParts[0] = numParts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    value = numParts.join('.');
  }

  input.value = value;
}

// Cached reporter minimum selector stake, in loya. null = not yet known / unavailable.
let REPORTER_MIN_LOYA = null;

// Fetch the reporter's on-chain min_tokens_required (loya) from the REST API.
// Returns a number, or null when it cannot be determined (validation is then skipped
// and the chain remains the source of truth). Result is cached after the first call.
async function fetchReporterMinLoya() {
  if (REPORTER_MIN_LOYA !== null) {
    return REPORTER_MIN_LOYA;
  }
  if (!/^https?:\/\//.test(LAYER_CHAIN_INFO.rest || '')) {
    return null;
  }
  try {
    const resp = await fetch(`${LAYER_CHAIN_INFO.rest}/tellor-io/layer/reporter/reporters`);
    if (!resp.ok) {
      return null;
    }
    const data = await resp.json();
    for (const r of (data.reporters || [])) {
      if (r.address !== REPORTER_ADDR) {
        continue;
      }
      const min = parseInt((r.metadata || {}).min_tokens_required || '0', 10);
      REPORTER_MIN_LOYA = Number.isFinite(min) && min > 0 ? min : null;
      return REPORTER_MIN_LOYA;
    }
    return null;
  } catch (e) {
    console.log('Could not fetch reporter minimum:', e.message);
    return null;
  }
}

// Format a loya amount as a trimmed TRB string (e.g. 10000000 -> "10").
function loyaToTRBString(loya) {
  return String(loya / Math.pow(10, DECIMALS));
}

// Open delegation modal
async function openDelegateModal() {
  const modal = document.getElementById('delegateModal');
  modal.classList.add('show');
  const amountInput = document.getElementById('delegateAmount');
  amountInput.value = '';
  amountInput.addEventListener('input', handleAmountInput);
  const statusEl = document.getElementById('modalStatus');
  statusEl.className = 'modal-status';
  statusEl.textContent = '';

  // Check if Keplr is installed and disable button if not
  const confirmBtn = document.getElementById('confirmBtn');
  if (!window.keplr) {
    confirmBtn.disabled = true;
  } else {
    confirmBtn.disabled = false;
  }

  // Fetch and display stake requirements (will show Keplr missing message if needed)
  loadStakeInfo();
}

// Load stake info when modal opens
async function loadStakeInfo() {
  const stakeInfoDiv = document.getElementById('stakeInfo');

  // Check if Keplr is installed first
  if (!window.keplr) {
    stakeInfoDiv.innerHTML = `
      <div style="text-align: center; padding: 20px; color: #fca5a5;">
        <div style="margin-bottom: 8px;">Missing Keplr wallet</div>
        <div style="font-size: 0.875rem; color: #cbd5e1;">
          <a href="https://www.keplr.app/" target="_blank" rel="noopener noreferrer" style="color: #ff6b6b; text-decoration: underline;">Click here to install</a>
        </div>
      </div>
    `;
    return;
  }

  const minLoya = await fetchReporterMinLoya();
  const minNote = minLoya
    ? `<div style="margin-top: 8px; color: #fbbf24;">This reporter requires more than <strong>${loyaToTRBString(minLoya)} TRB</strong> to select. Stake a little above it to cover staking rounding.</div>`
    : '';
  stakeInfoDiv.innerHTML = `
    <div style="text-align: center; padding: 10px; color: #94a3b8;">
      Enter the amount of TRB to delegate to our validator and select our reporter.
      ${minNote}
    </div>
  `;
}

// Close modal
function closeModal() {
  const modal = document.getElementById('delegateModal');
  modal.classList.remove('show');
}

// Show status message in modal
function showStatus(message, isError, isHtml = false) {
  const status = document.getElementById('modalStatus');
  if (isHtml) {
    status.innerHTML = message;
  } else {
    status.textContent = message;
  }
  if (isError) {
    status.className = 'modal-status error show';
  } else {
    status.className = 'modal-status success show';
  }
}

// Set button loading state
function setButtonLoading(btn, isLoading, text) {
  if (isLoading) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>' + text;
  } else {
    btn.disabled = false;
    btn.innerHTML = text;
  }
}

// Main delegation function using Direct signing (Protobuf)
async function executeDelegation() {
  const amountInput = document.getElementById('delegateAmount');
  const amount = parseFormattedNumber(amountInput.value);

  if (!amount || amount <= 0) {
    showStatus('Please enter a valid amount', true);
    return;
  }

  const confirmBtn = document.getElementById('confirmBtn');
  setButtonLoading(confirmBtn, true, 'Connecting...');

  try {
    if (!window.keplr) {
      showStatus('Missing Keplr wallet. <a href="https://www.keplr.app/" target="_blank" rel="noopener noreferrer" style="color: #ff6b6b; text-decoration: underline;">Click here to install</a>.', true, true);
      confirmBtn.disabled = true;
      confirmBtn.innerHTML = 'Confirm';
      return;
    }

    // Guard: Keplr's chain-suggest and the delegation fetches need public, browser
    // reachable HTTPS endpoints. If they are not configured, fail with a clear message
    // instead of suggesting an empty rpc to Keplr or fetching HTML (which then hits the
    // "Unexpected token '<'" JSON parse error).
    if (!/^https?:\/\//.test(LAYER_CHAIN_INFO.rpc || '') || !/^https?:\/\//.test(LAYER_CHAIN_INFO.rest || '')) {
      throw new Error('Chain endpoints not configured: the monitor has no public RPC/REST URL. Ask the operator to set PUBLIC_RPC_URL and PUBLIC_API_URL.');
    }

    console.log("=== DELEGATION DEBUG START ===");
    console.log("[1] Keplr detected, suggesting chain...");
    console.log("[1] Chain ID:", CHAIN_ID);
    console.log("[1] REST URL:", LAYER_CHAIN_INFO.rest);
    console.log("[1] Validator:", VALIDATOR_ADDR);
    console.log("[1] Reporter:", REPORTER_ADDR);

    // Suggest chain to Keplr
    try {
      await window.keplr.experimentalSuggestChain(LAYER_CHAIN_INFO);
      console.log("[2] Chain suggested successfully");
    } catch (e) {
      console.log("[2] Chain suggestion failed (continuing):", e.message);
    }

    await window.keplr.enable(CHAIN_ID);
    console.log("[3] Keplr enabled for chain");

    setButtonLoading(confirmBtn, true, 'Preparing...');

    // Use direct signer for protobuf signing
    const offlineSigner = window.keplr.getOfflineSigner(CHAIN_ID);
    const accounts = await offlineSigner.getAccounts();
    const sender = accounts[0].address;
    const pubkeyBytes = accounts[0].pubkey;
    console.log("[4] Connected wallet address:", sender);
    console.log("[4] Wallet pubkey:", btoa(String.fromCharCode(...pubkeyBytes)));

    const amountInLoya = Math.floor(amount * Math.pow(10, DECIMALS)).toString();
    console.log("[5] Amount to delegate:", amount, "TRB =", amountInLoya, "loya");

    // Fetch account info
    const accountURL = `${LAYER_CHAIN_INFO.rest}/cosmos/auth/v1beta1/accounts/${sender}`;
    console.log("[6] Fetching account from:", accountURL);
    const accountResp = await fetch(accountURL);
    let accountNumber = "0", sequence = "0";
    if (accountResp.status === 404) {
      // Account has never transacted on-chain yet — use defaults (valid for first tx)
      console.log("[6] Account not found on-chain (new account), using account_number=0, sequence=0");
    } else if (!accountResp.ok) {
      console.error("[6] Account fetch failed:", accountResp.status, accountResp.statusText);
      throw new Error('Failed to fetch account info');
    } else {
      const accountData = await accountResp.json();
      console.log("[6] Raw account response:", JSON.stringify(accountData, null, 2));
      const acc = accountData.account;
      if (acc && acc.base_account) {
        console.log("[7] Using base_account format");
        accountNumber = acc.base_account.account_number || "0";
        sequence = acc.base_account.sequence || "0";
      } else if (acc) {
        console.log("[7] Using direct account format");
        accountNumber = acc.account_number || "0";
        sequence = acc.sequence || "0";
      }
    }

    console.log("[7] Parsed account_number:", accountNumber);
    console.log("[7] Parsed sequence:", sequence);

    // A selector can select only one reporter, so re-selecting fails on-chain with
    // "selector already exists" and reverts the whole tx. Check the wallet's current
    // selection and adapt: add-stake-only if it already selects this reporter, block if
    // it selects a different one, otherwise delegate + select as a first-time delegator.
    let alreadySelectsTarget = false;
    try {
      const selResp = await fetch(`${LAYER_CHAIN_INFO.rest}/tellor-io/layer/reporter/selector-reporter/${sender}`);
      if (selResp.ok) {
        const current = (await selResp.json()).reporter || '';
        console.log("[7b] Current reporter selection:", current || "(none)");
        if (current === REPORTER_ADDR) {
          alreadySelectsTarget = true;
        } else if (current) {
          showStatus('This wallet already selects a different reporter. Switch or unselect it first, then delegate here.', true);
          setButtonLoading(confirmBtn, false, 'Confirm');
          return;
        }
      }
    } catch (e) {
      console.log("[7b] Selection check failed, treating as new selection:", e.message);
    }

    // The reporter's on-chain minimum only applies when selecting for the first time
    // (strict ">" because staking rounds down ~1 loya). Adding to an existing selection
    // has no minimum, so skip the check then.
    if (!alreadySelectsTarget) {
      const minLoya = await fetchReporterMinLoya();
      if (minLoya && Math.floor(amount * Math.pow(10, DECIMALS)) <= minLoya) {
        showStatus(`This reporter requires more than ${loyaToTRBString(minLoya)} TRB. Please enter a bit more than ${loyaToTRBString(minLoya)} TRB.`, true);
        setButtonLoading(confirmBtn, false, 'Confirm');
        return;
      }
    }

    // Build proto messages. Always delegate; only add the select message when this wallet
    // is not already selecting the target reporter.
    const msgDelegate = encodeMsgDelegate(sender, VALIDATOR_ADDR, DENOM, amountInLoya);
    const encodedMsgs = [encodeAny("/cosmos.staking.v1beta1.MsgDelegate", msgDelegate)];
    if (!alreadySelectsTarget) {
      const msgSelectReporter = encodeMsgSelectReporter(sender, REPORTER_ADDR);
      encodedMsgs.push(encodeAny("/layer.reporter.MsgSelectReporter", msgSelectReporter));
    } else {
      console.log("[8] Already selecting this reporter; delegate-only (adding stake).");
    }

    // Build TxBody
    const bodyBytes = encodeTxBody(encodedMsgs, "");
    console.log("[8] TxBody bytes length:", bodyBytes.length);

    // Build AuthInfo with SIGN_MODE_DIRECT
    const fee = encodeFee([{ denom: DENOM, amount: "10000" }], "400000");
    const signerInfo = encodeSignerInfoDirect(pubkeyBytes, sequence);
    const authInfoBytes = encodeAuthInfo([signerInfo], fee);
    console.log("[8] AuthInfo bytes length:", authInfoBytes.length);

    // Build SignDoc for direct signing
    const signDoc = {
      bodyBytes: bodyBytes,
      authInfoBytes: authInfoBytes,
      chainId: CHAIN_ID,
      accountNumber: Long.fromString(accountNumber)
    };

    console.log("[8] SignDoc prepared for direct signing");

    setButtonLoading(confirmBtn, true, 'Sign in Keplr...');

    // Sign with Direct (Protobuf)
    console.log("[9] Requesting Keplr direct signature...");
    const signResponse = await window.keplr.signDirect(CHAIN_ID, sender, signDoc);
    console.log("[9] Sign response received");

    const signature = signResponse.signature.signature;
    console.log("[9] Signature:", signature);

    setButtonLoading(confirmBtn, true, 'Broadcasting...');

    // Build TxRaw (ensure bytes are Uint8Array for proper encoding)
    const sigBytes = base64ToBytes(signature);
    const signedBodyBytes = ensureUint8Array(signResponse.signed.bodyBytes);
    const signedAuthInfoBytes = ensureUint8Array(signResponse.signed.authInfoBytes);
    const txRaw = encodeTxRaw(signedBodyBytes, signedAuthInfoBytes, [sigBytes]);
    const txBytesBase64 = bytesToBase64(txRaw);

    console.log("[10] TX bytes (base64):", txBytesBase64.substring(0, 100) + "...");
    console.log("[10] Broadcasting to:", `${LAYER_CHAIN_INFO.rest}/cosmos/tx/v1beta1/txs`);

    const broadcastResp = await fetch(`${LAYER_CHAIN_INFO.rest}/cosmos/tx/v1beta1/txs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tx_bytes: txBytesBase64,
        mode: "BROADCAST_MODE_SYNC"
      })
    });

    const result = await broadcastResp.json();
    console.log("[11] Broadcast response:", JSON.stringify(result, null, 2));

    if (!result.tx_response || !result.tx_response.txhash) {
      throw new Error(result.message || 'Failed to broadcast transaction');
    }

    const txHash = result.tx_response.txhash;
    console.log("[11] TX broadcasted, hash:", txHash);

    // Wait for transaction to be confirmed
    setButtonLoading(confirmBtn, true, 'Confirming...');
    showStatus(`Waiting for confirmation... TxHash: ${txHash.substring(0, 12)}...`, false);

    const txResult = await waitForTxConfirmation(txHash);
    console.log("[12] TX confirmed:", JSON.stringify(txResult, null, 2));

    if (txResult.code && txResult.code !== 0) {
      // Transaction failed - show error toast
      console.error("[12] TX execution failed with code:", txResult.code);
      const errorMsg = parseErrorMessage(txResult.raw_log || `Transaction failed with code ${txResult.code}`);
      showToast('error', 'Transaction Failed', errorMsg, txHash);
      showStatus('Transaction failed: ' + errorMsg, true);
      setButtonLoading(confirmBtn, false, 'Confirm');
      console.log("=== DELEGATION DEBUG END (FAILED) ===");
    } else {
      // Transaction succeeded - show success toast and refresh
      console.log("[12] SUCCESS! TxHash:", txHash);
      console.log("=== DELEGATION DEBUG END ===");
      showToast('success', 'Transaction Successful', 'Delegated & selected reporter successfully!', txHash);
      showStatus(`Success! TxHash: ${txHash.substring(0, 16)}...`, false);
      setButtonLoading(confirmBtn, false, 'Done!');
      confirmBtn.disabled = true;

      setTimeout(() => {
        closeModal();
        confirmBtn.disabled = false;
        confirmBtn.innerHTML = 'Confirm';
        window.location.reload();
      }, 3000);
    }

  } catch (error) {
    console.error("=== DELEGATION ERROR ===");
    console.error('Delegation error:', error);
    console.error('Error stack:', error.stack);
    const errorMsg = parseErrorMessage(error.message || 'Transaction failed');
    showToast('error', 'Transaction Error', errorMsg);
    showStatus('Error: ' + errorMsg, true);
    setButtonLoading(confirmBtn, false, 'Confirm');
  }
}

// Convert Amino signed tx to proto bytes for broadcast
function aminoToProtoBytes(signResponse) {
  const { signed, signature } = signResponse;
  const txBytes = encodeStdTx(signed, signature);
  return bytesToBase64(txBytes);
}

// Protobuf encoding helpers
const Protobuf = {
  varint(value) {
    if (typeof value === 'string') value = parseInt(value);
    const bytes = [];
    while (value > 127) {
      bytes.push((value & 0x7f) | 0x80);
      value >>>= 7;
    }
    bytes.push(value);
    return new Uint8Array(bytes);
  },

  string(value) {
    return new TextEncoder().encode(value);
  },

  lengthDelimited(fieldNum, data) {
    const tag = this.varint((fieldNum << 3) | 2);
    const len = this.varint(data.length);
    return this.concat([tag, len, data]);
  },

  varintField(fieldNum, value) {
    if (typeof value === 'string') value = parseInt(value);
    const tag = this.varint((fieldNum << 3) | 0);
    const val = this.varint(value);
    return this.concat([tag, val]);
  },

  concat(arrays) {
    const totalLen = arrays.reduce((sum, arr) => sum + arr.length, 0);
    const result = new Uint8Array(totalLen);
    let offset = 0;
    for (const arr of arrays) {
      result.set(arr, offset);
      offset += arr.length;
    }
    return result;
  }
};

// Encode MsgDelegate
function encodeMsgDelegate(delegatorAddress, validatorAddress, denom, amount) {
  const coinDenom = Protobuf.lengthDelimited(1, Protobuf.string(denom));
  const coinAmount = Protobuf.lengthDelimited(2, Protobuf.string(amount));
  const coin = Protobuf.concat([coinDenom, coinAmount]);

  const delegator = Protobuf.lengthDelimited(1, Protobuf.string(delegatorAddress));
  const validator = Protobuf.lengthDelimited(2, Protobuf.string(validatorAddress));
  const amountField = Protobuf.lengthDelimited(3, coin);

  return Protobuf.concat([delegator, validator, amountField]);
}

// Encode MsgSelectReporter
function encodeMsgSelectReporter(selectorAddress, reporterAddress) {
  const selector = Protobuf.lengthDelimited(1, Protobuf.string(selectorAddress));
  const reporter = Protobuf.lengthDelimited(2, Protobuf.string(reporterAddress));

  return Protobuf.concat([selector, reporter]);
}

// Encode Any type
function encodeAny(typeUrl, value) {
  const typeUrlField = Protobuf.lengthDelimited(1, Protobuf.string(typeUrl));
  const valueField = Protobuf.lengthDelimited(2, value);
  return Protobuf.concat([typeUrlField, valueField]);
}

// Encode TxBody
function encodeTxBody(messages, memo) {
  let body = new Uint8Array(0);
  for (const msg of messages) {
    body = Protobuf.concat([body, Protobuf.lengthDelimited(1, msg)]);
  }
  if (memo) {
    body = Protobuf.concat([body, Protobuf.lengthDelimited(2, Protobuf.string(memo))]);
  }
  return body;
}

// Encode Coin
function encodeCoin(denom, amount) {
  const denomField = Protobuf.lengthDelimited(1, Protobuf.string(denom));
  const amountField = Protobuf.lengthDelimited(2, Protobuf.string(amount));
  return Protobuf.concat([denomField, amountField]);
}

// Encode Fee
function encodeFee(amounts, gasLimit) {
  let fee = new Uint8Array(0);
  for (const coin of amounts) {
    const coinBytes = encodeCoin(coin.denom, coin.amount);
    fee = Protobuf.concat([fee, Protobuf.lengthDelimited(1, coinBytes)]);
  }
  fee = Protobuf.concat([fee, Protobuf.varintField(2, gasLimit)]);
  return fee;
}

// Encode SignerInfo with SIGN_MODE_LEGACY_AMINO_JSON
function encodeSignerInfoAmino(pubkey, sequence) {
  const pubkeyBytes = Protobuf.lengthDelimited(1, pubkey);
  const pubkeyAny = encodeAny("/cosmos.crypto.secp256k1.PubKey", pubkeyBytes);

  // ModeInfo - Single with SIGN_MODE_LEGACY_AMINO_JSON (127)
  const modeInfoSingle = Protobuf.varintField(1, 127);
  const modeInfoSingleWrapped = Protobuf.lengthDelimited(1, modeInfoSingle);

  let signerInfo = Protobuf.lengthDelimited(1, pubkeyAny);
  signerInfo = Protobuf.concat([signerInfo, Protobuf.lengthDelimited(2, modeInfoSingleWrapped)]);
  if (parseInt(sequence) > 0) {
    signerInfo = Protobuf.concat([signerInfo, Protobuf.varintField(3, sequence)]);
  }

  return signerInfo;
}

// Encode SignerInfo with SIGN_MODE_DIRECT (1)
function encodeSignerInfoDirect(pubkey, sequence) {
  const pubkeyBytes = Protobuf.lengthDelimited(1, pubkey);
  const pubkeyAny = encodeAny("/cosmos.crypto.secp256k1.PubKey", pubkeyBytes);

  // ModeInfo - Single with SIGN_MODE_DIRECT (1)
  const modeInfoSingle = Protobuf.varintField(1, 1);
  const modeInfoSingleWrapped = Protobuf.lengthDelimited(1, modeInfoSingle);

  let signerInfo = Protobuf.lengthDelimited(1, pubkeyAny);
  signerInfo = Protobuf.concat([signerInfo, Protobuf.lengthDelimited(2, modeInfoSingleWrapped)]);
  if (parseInt(sequence) > 0) {
    signerInfo = Protobuf.concat([signerInfo, Protobuf.varintField(3, sequence)]);
  }

  return signerInfo;
}

// Encode AuthInfo
function encodeAuthInfo(signerInfos, fee) {
  let authInfo = new Uint8Array(0);
  for (const si of signerInfos) {
    authInfo = Protobuf.concat([authInfo, Protobuf.lengthDelimited(1, si)]);
  }
  authInfo = Protobuf.concat([authInfo, Protobuf.lengthDelimited(2, fee)]);
  return authInfo;
}

// Encode TxRaw
function encodeTxRaw(bodyBytes, authInfoBytes, signatures) {
  let txRaw = Protobuf.lengthDelimited(1, bodyBytes);
  txRaw = Protobuf.concat([txRaw, Protobuf.lengthDelimited(2, authInfoBytes)]);
  for (const sig of signatures) {
    txRaw = Protobuf.concat([txRaw, Protobuf.lengthDelimited(3, sig)]);
  }
  return txRaw;
}

// Encode StdTx to proto bytes (handles multiple messages)
function encodeStdTx(signed, signature) {
  const encodedMsgs = [];

  for (const msg of signed.msgs) {
    if (msg.type === "cosmos-sdk/MsgDelegate") {
      const msgDelegate = encodeMsgDelegate(
        msg.value.delegator_address,
        msg.value.validator_address,
        msg.value.amount.denom,
        msg.value.amount.amount
      );
      encodedMsgs.push(encodeAny("/cosmos.staking.v1beta1.MsgDelegate", msgDelegate));
    } else if (msg.type === "layer/MsgSelectReporter") {
      const msgSelectReporter = encodeMsgSelectReporter(
        msg.value.selector_address,
        msg.value.reporter_address
      );
      encodedMsgs.push(encodeAny("/layer.reporter.MsgSelectReporter", msgSelectReporter));
    }
  }

  const bodyBytes = encodeTxBody(encodedMsgs, signed.memo);

  const fee = encodeFee(signed.fee.amount, signed.fee.gas);

  const pubkeyBytes = base64ToBytes(signature.pub_key.value);
  const signerInfo = encodeSignerInfoAmino(pubkeyBytes, signed.sequence);

  const authInfoBytes = encodeAuthInfo([signerInfo], fee);

  const sigBytes = base64ToBytes(signature.signature);

  return encodeTxRaw(bodyBytes, authInfoBytes, [sigBytes]);
}

// Base64 helpers
function base64ToBytes(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function bytesToBase64(bytes) {
  let binary = '';
  const arr = ensureUint8Array(bytes);
  for (let i = 0; i < arr.length; i++) {
    binary += String.fromCharCode(arr[i]);
  }
  return btoa(binary);
}

// Ensure bytes are Uint8Array (handles plain arrays from Keplr)
function ensureUint8Array(bytes) {
  if (bytes instanceof Uint8Array) return bytes;
  return new Uint8Array(bytes);
}

// Wait for transaction to be confirmed on chain
async function waitForTxConfirmation(txHash, maxAttempts = 30, intervalMs = 2000) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const resp = await fetch(`${LAYER_CHAIN_INFO.rest}/cosmos/tx/v1beta1/txs/${txHash}`);
      if (resp.ok) {
        const data = await resp.json();
        if (data.tx_response && data.tx_response.height && parseInt(data.tx_response.height) > 0) {
          return data.tx_response;
        }
      }
    } catch (e) {
      console.log(`[waitForTx] Attempt ${i + 1}: ${e.message}`);
    }
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }
  throw new Error('Transaction confirmation timeout');
}

// Parse error message to make it more user-friendly
function parseErrorMessage(rawLog) {
  if (!rawLog) return 'Unknown error';

  // Common error patterns
  if (rawLog.includes('min requirement') && rawLog.includes('not met')) {
    const match = rawLog.match(/Must stake (\d+) more/);
    if (match) {
      const amount = parseInt(match[1]) / 1000000;
      return `Minimum stake requirement not met. Need ${amount.toFixed(2)} more TRB.`;
    }
    return 'Minimum stake requirement not met for this reporter.';
  }

  if (rawLog.includes('insufficient funds')) {
    return 'Insufficient balance for this transaction.';
  }

  if (rawLog.includes('signature verification failed')) {
    return 'Signature verification failed. Please try again.';
  }

  // Truncate if too long
  if (rawLog.length > 150) {
    return rawLog.substring(0, 150) + '...';
  }

  return rawLog;
}

// Show toast notification
function showToast(type, title, message, txHash) {
  // Remove existing toast if any
  const existingToast = document.getElementById('txToast');
  if (existingToast) existingToast.remove();

  const toast = document.createElement('div');
  toast.id = 'txToast';
  toast.className = `tx-toast tx-toast-${type}`;

  const icon = type === 'success' ? '&#10004;' : '&#10006;';
  const explorerUrl = txHash && EXPLORER_URL ? `${EXPLORER_URL}/tx/${txHash}` : '#';

  toast.innerHTML = `
    <div class="tx-toast-header">
      <span class="tx-toast-icon">${icon}</span>
      <span class="tx-toast-title">${title}</span>
      <button class="tx-toast-close" onclick="this.parentElement.parentElement.remove()">&times;</button>
    </div>
    <div class="tx-toast-body">
      <p>${message}</p>
      ${txHash ? `<a href="${explorerUrl}" target="_blank" class="tx-toast-link">View on Explorer &rarr;</a>` : ''}
    </div>
  `;

  document.body.appendChild(toast);

  // Auto-remove after 10 seconds for success, keep error visible
  if (type === 'success') {
    setTimeout(() => {
      if (toast.parentElement) toast.remove();
    }, 10000);
  }
}

// Initialize event listeners
document.addEventListener('DOMContentLoaded', function() {
  const modal = document.getElementById('delegateModal');
  if (modal) {
    modal.addEventListener('click', function(e) {
      if (e.target === this) closeModal();
    });
  }

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeModal();
  });
});
