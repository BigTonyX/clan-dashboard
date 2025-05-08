// --- script.js loaded ---
console.log("--- script.js loaded ---");

// --- DOM Elements ---
const countdownTimerElement = document.getElementById('countdown-timer');
const lastUpdatedElement = document.getElementById('last-updated');
const leaderboardBody = document.getElementById('leaderboard-body');
const gainHeaderElement = document.getElementById('gain-header'); 
const comparisonTimePeriodRadios = document.querySelectorAll('input[name="comparison_time_period"]');
const updateComparisonBtn = document.getElementById('update-comparison-btn');
const comparisonChartCanvas = document.getElementById('comparison-chart');
const comparisonSelectionArea = document.getElementById('comparison-selection-area');
const selectedClanPills = document.getElementById('selected-clan-pills');
const comparisonClanListDiv = document.getElementById('comparison-clan-list');
const allClanPillsContainer = document.getElementById('all-clan-pills'); 

// Controls
const controlsArea = document.getElementById('controls-area'); 
const controlsHeading = controlsArea.querySelector('h2');
const timePeriodRadios = document.querySelectorAll('input[name="time_period"]');
const forecastPeriodRadios = document.querySelectorAll('input[name="forecast_period"]');
const targetClanSelect = document.getElementById('target-clan-select');
const targetRankInput = document.getElementById('target-rank-input');

// Display Area
const reachTargetDisplay = document.getElementById('reach-target-display');

// --- State Variables ---
let currentTimePeriod = 60;
let currentForecastPeriod = 360;
let currentTargetClan = '';
let currentTargetRank = 1;
let currentBattleId = null; // Will be set to the latest battle from API
let comparisonChart = null; // Variable to hold the chart instance
let selectedComparisonClans = []; // Array to hold selected clan names
let currentComparisonTimePeriod = 60; // Default comparison period
let controlsExpanded = true; // Initial state of controls
let clanList = []; // Global clan list for dropdown and other uses
let battleList = []; // store fetched battles for custom dropdown
let pendingBattleId = null;
let pendingTargetClan = null;
let pendingTargetRank = null;
let lastDashboardData = null; // Store the last loaded dashboard data
let nextRefreshTime = null; // Track when the next refresh will occur

// --- API Base URL ---
//const API_BASE_URL = "http://127.0.0.1:8000pi/clan"; // Local server for testing with correct path prefix
const API_BASE_URL = "https://clan-dashboard-api.onrender.com/api/clan"; // Production Render server

// --- Battle Selector Population ---
async function populateBattleSelector() {
    const battleSelect = document.getElementById('battle-select');
    if (!battleSelect) {
        console.error("Battle select element not found!");
        return;
    }

    try {
        console.log("Fetching battle IDs...");
        const response = await fetch(`${API_BASE_URL}/api/battle_ids`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const battles = await response.json();
        
        battleList = battles; // save for custom dropdown
        // Clear existing options
        battleSelect.innerHTML = '';
        
        // Get stored battle ID from localStorage
        const storedBattleId = localStorage.getItem('selectedBattleId');
        
        // Add battles to selector
        battles.forEach(battle => {
            const option = document.createElement('option');
            option.value = battle.battle_id;
            option.textContent = battle.battle_id;
            battleSelect.appendChild(option);
        });

        // Try to use stored battle ID if it exists in the options
        if (storedBattleId && Array.from(battleSelect.options).some(opt => opt.value === storedBattleId)) {
            battleSelect.value = storedBattleId;
            currentBattleId = storedBattleId;
        } else if (battles.length > 0) {
            // Fall back to most recent battle (battles[0])
            currentBattleId = battles[0].battle_id;
            battleSelect.value = currentBattleId;
        }

        console.log(`Populated battle selector with ${battles.length} battles. Current battle: ${currentBattleId}`);

        if (currentBattleId) {
            localStorage.setItem('selectedBattleId', currentBattleId);
        }

        // Build custom dropdown for battles
        renderCustomBattleDropdown(battles);
    } catch (error) {
        console.error("Error populating battle selector:", error);
        battleSelect.innerHTML = '<option value="">Error loading battles</option>';
    }
}

// --- Custom Battle Dropdown ---
function renderCustomBattleDropdown(battles) {
    const dropdown = document.getElementById('custom-battle-dropdown');
    if (!dropdown) {
        console.error('custom-battle-dropdown not found');
        return;
    }
    dropdown.innerHTML = '';
    // Use pendingBattleId if set, otherwise currentBattleId
    const selectedBattleId = pendingBattleId || currentBattleId;
    const selected = document.createElement('div');
    selected.className = 'custom-clan-selected';
    selected.innerHTML = `<span>${selectedBattleId || ''}</span><span class="dropdown-arrow">&#9662;</span>`;
    dropdown.appendChild(selected);
    // List
    const list = document.createElement('ul');
    list.className = 'clan-dropdown-list';
    battles.forEach(({ battle_id }) => {
        const item = document.createElement('li');
        item.className = 'clan-dropdown-item';
        item.textContent = battle_id;
        if (battle_id === selectedBattleId) {
            item.classList.add('selected');
        }
        item.onclick = () => {
            pendingBattleId = battle_id;
            renderCustomBattleDropdown(battles);
        };
        list.appendChild(item);
    });
    dropdown.appendChild(list);
    // Toggle list
    selected.onclick = (e) => {
        e.stopPropagation();
        list.classList.toggle('show');
    };
    document.addEventListener('click', () => list.classList.remove('show'));
    // Match list width
    setTimeout(() => {
        const w = selected.offsetWidth;
        list.style.width = w + 'px';
        list.style.minWidth = w + 'px';
        list.style.boxSizing = 'border-box';
    }, 0);
}

// --- Fetch Countdown Data ---
async function fetchCountdown() {
    // console.log("Fetching countdown..."); // Reduce logging
    try {
        const response = await fetch(`${API_BASE_URL}/countdown`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        countdownTimerElement.textContent = (data && data.countdown) ? data.countdown : "N/A";
    } catch (error) {
        console.error("Error fetching or processing countdown:", error);
        countdownTimerElement.textContent = "Error";
    }
}

function renderAllClanPills() {
    if (!clanList || !allClanPillsContainer) return;
    allClanPillsContainer.innerHTML = '';
    clanList.forEach(clan => {
      const pill = document.createElement('span');
      pill.classList.add('clan-option-pill');
      pill.textContent = clan.clan_name;
      pill.dataset.clanName = clan.clan_name;
      pill.addEventListener('click', handleClanPillClick);
      if (selectedComparisonClans.includes(clan.clan_name)) {
        pill.classList.add('selected');
      }
      allClanPillsContainer.appendChild(pill);
    });
}

function handleClanPillClick(event) {
    const clickedElement = event.target;
    const clanName = clickedElement.dataset.clanName;
  
    if (clickedElement.classList.contains('clan-pill')) {
      // Clicked a pill in the selected-clan-pills area (top)
      selectedComparisonClans = selectedComparisonClans.filter(clan => clan !== clanName);
      renderClanPills(); // Re-render the selected pills
      renderAllClanPills(); // Re-render all pills to update their states
    } else if (clickedElement.classList.contains('clan-option-pill')) {
      // Clicked a clan pill in the main list
      const isSelected = selectedComparisonClans.includes(clanName);
      if (!isSelected) {
        if (selectedComparisonClans.length < 3) {
          selectedComparisonClans.push(clanName);
          renderClanPills(); // Re-render the selected pills
          renderAllClanPills(); // Re-render all pills to update their states
        } else {
          alert("You can only select up to 3 clans for comparison.");
        }
      }
    }
    console.log("Selected comparison clans:", selectedComparisonClans);
}

function renderClanPills() {
    selectedClanPills.innerHTML = '';
    selectedComparisonClans.forEach(clanName => {
      const pill = document.createElement('span');
      pill.classList.add('clan-pill', 'selected'); // Style for selected pills
      pill.textContent = clanName;
      pill.dataset.clanName = clanName; // Store clan name for removal
      pill.addEventListener('click', handleClanPillClick); // Single click to remove
      selectedClanPills.appendChild(pill);
    });
}
  

function updateCheckboxStates() {
    const checkboxes = document.querySelectorAll('#comparison-clan-list input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
      checkbox.disabled = selectedComparisonClans.length >= 3 && !selectedComparisonClans.includes(checkbox.value);
    });
}


// --- Formatting Helpers ---
function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return num.toLocaleString();
}

function formatGain(num) {
    if (num === null || num === undefined) return '-';
    const formattedNum = num.toLocaleString();
    return num > 0 ? `+${formattedNum}` : formattedNum;
}

function formatTimeOrNA(value) {
    if (value === null || value === undefined || value === "N/A" || value === "Error") return '-';
    return value;
}

function formatRank(rank) {
    if (rank === null || rank === undefined) return '-';
    return rank;
}

// --- ** NEW Helper: Format Time Period for Header ** ---
function formatTimePeriodHeader(minutes) {
    if (minutes < 60) {
        return `Gain (${minutes}m)`;
    } else if (minutes === 1440) { // Handle 24h specifically
        return `Gain (24h)`;
    } else {
        const hours = minutes / 60;
        return `Gain (${hours}h)`;
    }
}

// --- Fetch and Display "Reach Target" Data ---
async function fetchReachTargetData(suppressPlaceholder = false) {
    if (!currentTargetClan) {
        if (!suppressPlaceholder) {
            reachTargetDisplay.innerHTML = '<p>Select a Clan to Track in Controls</p>';
        }
        return;
    }
    reachTargetDisplay.innerHTML = '<p>Calculating...</p>';
    console.log(`Fetching reach target data for ${currentTargetClan} to rank ${currentTargetRank} (forecast period ${currentForecastPeriod}m)...`);

    const url = `${API_BASE_URL}/clan_reach_target?clan_name=${encodeURIComponent(currentTargetClan)}&target_rank=${currentTargetRank}&forecast_period=${currentForecastPeriod}&battle_id=${encodeURIComponent(currentBattleId)}`;

    try {
        const response = await fetch(url);
        const data = await response.json();

        if (!response.ok) {
            const detail = data.detail || `HTTP error! status: ${response.status}`;
            throw new Error(detail);
        }

        let resultText = "Error";
        if (data && data.extra_points_per_hour !== undefined) {
             const points = data.extra_points_per_hour;
             if (points === null && data.final_rank !== undefined && data.final_rank !== null) {
                 // War is over, show final rank
                 let suffix = 'th';
                 if (data.final_rank === 1) suffix = 'st';
                 else if (data.final_rank === 2) suffix = 'nd';
                 else if (data.final_rank === 3) suffix = 'rd';
                 resultText = `War Over, Finished ${data.final_rank}${suffix}`;
             } else if (points === null) { resultText = `Ineligible (needs >6hrs data)`; }
             else if (points === 0) { resultText = `Already projected >= rank ${currentTargetRank}!`; }
             else if (points === "Infinity") { resultText = `Needs infinite points/hr`; }
             else { resultText = `Needs *${formatNumber(points)}* extra pts/hr for rank ${currentTargetRank}`; }
        }
         reachTargetDisplay.innerHTML = `<p><strong>${currentTargetClan}</strong>: ${resultText}</p>`;

    } catch (error) {
         console.error("Error fetching reach target data:", error);
         reachTargetDisplay.innerHTML = `<p>Error calculating for ${currentTargetClan}: ${error.message}</p>`;
    }
}


// --- Fetch Dashboard Data (and update controls/highlighting) ---
async function fetchDashboardData() {
    console.log(`Fetching dashboard data (snapshot) for battle_id=${currentBattleId}...`);
    const url = `${API_BASE_URL}/dashboard?battle_id=${encodeURIComponent(currentBattleId)}`;
    leaderboardBody.innerHTML = '<tr><td colspan="7">Loading...</td></tr>'; // Show loading state

    try {
        const response = await fetch(url);
        if (!response.ok) {
            let errorDetail = `HTTP error! status: ${response.status}`;
            try {
                const errorData = await response.json();
                errorDetail += ` - ${JSON.stringify(errorData.detail || errorData)}`;
            } catch (jsonError) { /* Ignore if response wasn't JSON */ }
            throw new Error(errorDetail);
        }
        const topClans = await response.json();
        lastDashboardData = topClans; // Cache the data for re-rendering
        console.log("Snapshot data received (first few):", topClans.slice(0, 3));

        // Set next refresh time
        nextRefreshTime = new Date(Date.now() + 120000); // 2 minutes from now
        const currentTime = new Date().toLocaleTimeString();
        lastUpdatedElement.setAttribute('data-last-updated', currentTime);
        updateRefreshCountdown();

        if (topClans && Array.isArray(topClans) && topClans.length > 0) {
            // Update global clanList with the latest snapshot data
            clanList = topClans;
            // --- Populate Target Clan Dropdown (Existing logic) ---
            const previousSelectedClan = targetClanSelect.value;
            let wasUnset = !currentTargetClan;
            targetClanSelect.innerHTML = '<option value="">-- Select Clan --</option>';
            topClans.forEach(clan => {
                const option = document.createElement('option');
                option.value = clan.clan_name; option.textContent = clan.clan_name;
                if (clan.clan_name === previousSelectedClan) {
                    option.selected = true;
                }
                targetClanSelect.appendChild(option);
            });
            const savedClan = localStorage.getItem('selectedClan');
            let validSavedClan = savedClan && Array.from(targetClanSelect.options).some(opt => opt.value === savedClan);

            if (validSavedClan) {
                targetClanSelect.value = savedClan;
                currentTargetClan = savedClan;
            } else if (!previousSelectedClan) {
                const nongOption = Array.from(targetClanSelect.options).find(opt => opt.value === "NONG");
                if (nongOption) {
                    targetClanSelect.value = "NONG";
                    currentTargetClan = "NONG";
                    localStorage.setItem('selectedClan', "NONG");
                } else if (targetClanSelect.options.length > 1) {
                    targetClanSelect.selectedIndex = 1;
                    currentTargetClan = targetClanSelect.value;
                    localStorage.setItem('selectedClan', currentTargetClan);
                }
            } else {
                targetClanSelect.value = previousSelectedClan;
                currentTargetClan = previousSelectedClan;
            }
            // --- End Dropdown Population ---

            // --- Populate All Clan Pills ---
            if (allClanPillsContainer) {
                allClanPillsContainer.innerHTML = '';
                topClans.forEach(clan => {
                    const pill = document.createElement('span');
                    pill.classList.add('clan-option-pill');
                    pill.textContent = clan.clan_name;
                    pill.dataset.clanName = clan.clan_name;
                    pill.addEventListener('click', handleClanPillClick);
                    if (selectedComparisonClans.includes(clan.clan_name)) {
                        pill.classList.add('selected');
                    }
                    allClanPillsContainer.appendChild(pill);
                });
            } else {
                console.error("All clan pills container not found!");
            }
            // --- End Populate All Clan Pills ---

            // --- Populate table rows (Snapshot logic) ---
            let trackedClanGain = null;
            if (topClans && Array.isArray(topClans)) {
                const trackedClan = topClans.find(clan => clan.clan_name === currentTargetClan);
                if (trackedClan) {
                    const gainField = `gain_${currentTimePeriod}m`;
                    trackedClanGain = trackedClan[gainField];
                }
            }

            // Update the gain header to reflect the selected period
            updateGainHeader(currentTimePeriod);

            // --- Calculate gap and time to catch for each clan ---
            const gainField = `gain_${currentTimePeriod}m`;
            const forecastGainField = `gain_${currentForecastPeriod}m`;
            for (let i = 0; i < topClans.length; i++) {
                const clan = topClans[i];
                let gap = '';
                let timeToCatch = '';
                if (i > 0) {
                    // Calculate gap to the clan above
                    const aboveClan = topClans[i - 1];
                    if (aboveClan && typeof aboveClan.current_points === 'number' && typeof clan.current_points === 'number') {
                        gap = aboveClan.current_points - clan.current_points;
                        if (gap < 0) gap = 0;
                    }
                    // Calculate time to catch using forecast period gains
                    const currentForecastGain = clan[forecastGainField];
                    const aboveForecastGain = aboveClan[forecastGainField];
                    if (
                        typeof currentForecastGain === 'number' &&
                        typeof aboveForecastGain === 'number' &&
                        currentForecastGain > aboveForecastGain &&
                        gap > 0 &&
                        currentForecastPeriod > 0
                    ) {
                        const gainDifference = currentForecastGain - aboveForecastGain;
                        const minutesToCatch = (gap * currentForecastPeriod) / gainDifference;
                        if (isFinite(minutesToCatch) && minutesToCatch > 0) {
                            // Format as days/hours/minutes if over 24h
                            const days = Math.floor(minutesToCatch / 1440);
                            const hours = Math.floor((minutesToCatch % 1440) / 60);
                            const minutes = Math.round(minutesToCatch % 60);
                            if (days > 0) {
                                let str = `${days}d`;
                                if (hours > 0) str += ` ${hours}h`;
                                if (minutes > 0) str += ` ${minutes}m`;
                                timeToCatch = str;
                            } else if (hours > 0 && minutes > 0) {
                                timeToCatch = `${hours}h ${minutes}m`;
                            } else if (hours > 0) {
                                timeToCatch = `${hours}h`;
                            } else {
                                timeToCatch = `${minutes}m`;
                            }
                        }
                    }
                }
                // Render the row
                const row = document.createElement('tr');
                if (clan.clan_name === currentTargetClan) {
                    row.classList.add('highlight');
                }
                const gainValue = clan[gainField] !== undefined ? clan[gainField] : null;
                const imageId = getImageId(clan.icon);
                const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';
                // --- Add gain-higher-than-tracked class if needed ---
                let gainClass = '';
                if (
                    trackedClanGain !== null &&
                    gainValue !== null &&
                    clan.clan_name !== currentTargetClan &&
                    gainValue > trackedClanGain
                ) {
                    gainClass = 'gain-higher-than-tracked';
                }


            }
            // Update Last Updated timestamp
            lastUpdatedElement.textContent = new Date().toLocaleTimeString();

            // After setting currentTargetClan, if it was previously unset and is now set, call fetchReachTargetData
            if (wasUnset && currentTargetClan) {
                fetchReachTargetData();
            }
        } else {
            // Handle empty data case
            console.warn("Received empty or invalid topClans list:", topClans);
            leaderboardBody.innerHTML = '<tr><td colspan="7">No clan data available.</td></tr>';
            if (comparisonClanListDiv) comparisonClanListDiv.innerHTML = '(No clans to select)';
            lastUpdatedElement.textContent = new Date().toLocaleTimeString();
        }

        // --- Restore Custom Dropdown Rendering ---
        // 1. Hide the old select and add a new div for the custom dropdown
        console.log('Attempting to hide old select and insert custom dropdown...');
        if (targetClanSelect) {
            targetClanSelect.style.display = 'none';
            console.log('targetClanSelect found and hidden.');
        } else {
            console.warn('targetClanSelect not found!');
        }
        // Always remove the old custom dropdown before creating a new one
        let oldDropdown = document.getElementById('custom-clan-dropdown');
        if (oldDropdown && oldDropdown.parentNode) {
            oldDropdown.parentNode.removeChild(oldDropdown);
        }
        let customDropdown = document.createElement('div');
        customDropdown.id = 'custom-clan-dropdown';
        customDropdown.className = 'custom-clan-dropdown';
        if (targetClanSelect && targetClanSelect.parentNode) {
            targetClanSelect.parentNode.insertBefore(customDropdown, targetClanSelect);
            console.log('Inserted custom dropdown into DOM.');
        } else {
            console.warn('Could not insert custom dropdown: targetClanSelect or its parentNode is missing.');
        }

        // Helper to render the custom dropdown
        console.log('Calling renderCustomClanDropdown with topClans:', topClans);
        renderCustomClanDropdown(topClans);

        // Hide the old number input and add a new div for the custom rank dropdown
        if (targetRankInput) targetRankInput.style.display = 'none';
        let customRankDropdown = document.getElementById('custom-rank-dropdown');
        if (!customRankDropdown) {
            customRankDropdown = document.createElement('div');
            customRankDropdown.id = 'custom-rank-dropdown';
            customRankDropdown.className = 'custom-rank-dropdown';
            targetRankInput.parentNode.insertBefore(customRankDropdown, targetRankInput);
        }

        // In fetchDashboardData or initializeApp, after rendering the controls, call:
        renderCustomRankDropdown(currentTargetRank);

        renderLeaderboardTable(topClans); // Only call this to render the table
    } catch (error) {
        console.error("Error fetching or processing snapshot data:", error);
        leaderboardBody.innerHTML = `<tr><td colspan="7">Error loading data. Check console. (${error.message})</td></tr>`;
        if (comparisonClanListDiv) comparisonClanListDiv.innerHTML = '(Error loading clans)';
        lastUpdatedElement.textContent = new Date().toLocaleTimeString();
    }
}

// Helper to render the custom dropdown
function renderCustomClanDropdown(clanList) {
    console.log('Rendering custom clan dropdown. clanList:', clanList);
    const dropdown = document.getElementById('custom-clan-dropdown');
    if (!dropdown) {
        console.error('custom-clan-dropdown div not found!');
        return;
    }
    dropdown.innerHTML = '';
    // Use pendingTargetClan if set, otherwise currentTargetClan
    const selectedClanName = pendingTargetClan || currentTargetClan;
    let selectedClan = clanList.find(c => c.clan_name === selectedClanName) || clanList[0];
    const imageId = getImageId(selectedClan.icon);
    const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';
    const selected = document.createElement('div');
    selected.className = 'custom-clan-selected';
    selected.innerHTML = `
        ${imageUrl ? `<img src="${imageUrl}" class="clan-icon">` : ''}
        <span>${selectedClan.clan_name}</span>
        <span class="dropdown-arrow">&#9662;</span>
    `;
    dropdown.appendChild(selected);

    const list = document.createElement('ul');
    list.className = 'clan-dropdown-list';
    clanList.forEach(clan => {
        const imageId = getImageId(clan.icon);
        const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';
        const item = document.createElement('li');
        item.className = 'clan-dropdown-item';
        item.innerHTML = `
            ${imageUrl ? `<img src="${imageUrl}" class="clan-icon">` : ''}
            <span>${clan.clan_name}</span>
        `;
        if (clan.clan_name === selectedClanName) {
            item.classList.add('selected');
        }
        item.onclick = () => {
            pendingTargetClan = clan.clan_name;
            targetClanSelect.value = clan.clan_name; // Sync native select with custom dropdown
            renderCustomClanDropdown(clanList);
        };
        list.appendChild(item);
    });
    dropdown.appendChild(list);

    // Force dropdown list width to match the selected box (edge-to-edge)
    setTimeout(() => {
        const selectedBox = dropdown.querySelector('.custom-clan-selected');
        if (selectedBox) {
            const selectedWidth = selectedBox.offsetWidth;
            list.style.width = selectedWidth + 'px';
            list.style.minWidth = selectedWidth + 'px';
            list.style.maxWidth = selectedWidth + 'px';
            list.style.boxSizing = 'border-box';
        }
    }, 0);

    // Toggle dropdown
    selected.onclick = (e) => {
        e.stopPropagation();
        list.classList.toggle('show');
        console.log('Dropdown toggled.');
    };
    document.addEventListener('click', () => list.classList.remove('show'));
}

function renderCustomRankDropdown(selectedRank = 1) {
    const dropdown = document.getElementById('custom-rank-dropdown');
    dropdown.innerHTML = '';
    // Use pendingTargetRank if set, otherwise currentTargetRank
    const selectedRankValue = pendingTargetRank || currentTargetRank;
    const selected = document.createElement('div');
    selected.className = 'custom-rank-selected';
    selected.innerHTML = `
        <span>${selectedRankValue}</span>
        <span class="dropdown-arrow">&#9662;</span>
    `;
    dropdown.appendChild(selected);

    const list = document.createElement('ul');
    list.className = 'rank-dropdown-list';
    for (let i = 1; i <= 20; i++) {
        const item = document.createElement('li');
        item.className = 'rank-dropdown-item';
        item.textContent = i;
        if (i === selectedRankValue) {
            item.classList.add('selected');
        }
        item.onclick = () => {
            pendingTargetRank = i;
            renderCustomRankDropdown(i);
        };
        list.appendChild(item);
    }
    dropdown.appendChild(list);

    // Force dropdown list width to match the selected box
    setTimeout(() => {
        const selectedBox = dropdown.querySelector('.custom-rank-selected');
        if (selectedBox) {
            const selectedWidth = selectedBox.offsetWidth;
            list.style.width = selectedWidth + 'px';
            list.style.minWidth = selectedWidth + 'px';
            list.style.maxWidth = selectedWidth + 'px';
            list.style.boxSizing = 'border-box';
        }
    }, 0);

    // Toggle dropdown
    selected.onclick = (e) => {
        e.stopPropagation();
        list.classList.toggle('show');
    };
    document.addEventListener('click', () => list.classList.remove('show'));
}

// --- Fetch Comparison Data & Prepare Chart ---
async function updateComparisonChart() {
    console.log("--- updateComparisonChart function START ---"); // Log function start
    console.log("Selected clans:", selectedComparisonClans);
    console.log("Selected time period:", currentComparisonTimePeriod);

    // Validate selection
    if (!selectedComparisonClans || selectedComparisonClans.length === 0 || selectedComparisonClans.length > 3) {
        alert("Please select between 1 and 3 clans to compare.");
        return;
    }

    // --- Construct URL Correctly ---
    // Start with the base API endpoint
    let comparisonUrl = `${API_BASE_URL}/clan_comparison?time_period=${currentComparisonTimePeriod}&battle_id=${encodeURIComponent(currentBattleId)}`;
    // Add each selected clan name as a separate parameter
    selectedComparisonClans.forEach(name => {
        comparisonUrl += `&clan_names=${encodeURIComponent(name)}`;
    });
    // --- End URL Construction ---

    console.log("Fetching comparison data:", comparisonUrl); // Check the constructed URL
    const ctx = comparisonChartCanvas.getContext('2d');
    // Clear previous chart/message and show loading text
    if (comparisonChart) {
        comparisonChart.destroy();
        comparisonChart = null;
    }
    ctx.clearRect(0, 0, comparisonChartCanvas.width, comparisonChartCanvas.height);
    ctx.font = "16px sans-serif"; ctx.fillStyle = "#888"; // Adjust color if needed
    ctx.fillText("Loading chart data...", 10, 50);

    try {
        const response = await fetch(comparisonUrl); // Use the correctly built URL
        if (!response.ok) {
             let errorDetail = `HTTP error! status: ${response.status}`;
             try { const errorData = await response.json(); errorDetail += ` - ${JSON.stringify(errorData.detail || errorData)}`;} catch (jsonError) {}
             throw new Error(errorDetail);
        }
        const historyData = await response.json();
        console.log("Comparison data received:", historyData);

        // Process historyData and render Chart.js chart
        renderComparisonChart(historyData); // Call the function to draw/update

    } catch (error) {
        console.error("Error fetching or processing comparison data:", error);
        alert(`Error fetching comparison data: ${error.message}`);
        // Display error on canvas
        if (comparisonChart) { comparisonChart.destroy(); comparisonChart = null; }
        ctx.clearRect(0, 0, comparisonChartCanvas.width, comparisonChartCanvas.height);
        ctx.fillText(`Error loading chart: ${error.message}`, 10, 50);
    }
}


// --- Render Chart Function ---
function renderComparisonChart(data) {
    console.log("Processing data and rendering chart...");
    const ctx = comparisonChartCanvas.getContext('2d');

    // Check if data is valid
    if (!data || !Array.isArray(data)) {
        console.error("Invalid data received for chart rendering.");
         ctx.clearRect(0, 0, comparisonChartCanvas.width, comparisonChartCanvas.height);
         ctx.fillText("Invalid chart data received.", 10, 50);
        return;
    }
    // Handle case where API returns data, but it's empty for the selection/period
    if (data.length === 0 && selectedComparisonClans.length > 0) {
         console.log("No historical data found for selected clans/period.");
         ctx.clearRect(0, 0, comparisonChartCanvas.width, comparisonChartCanvas.height);
         ctx.fillText("No historical data found for this selection.", 10, 50);
         // Destroy old chart if it exists and no new data found
         if (comparisonChart) { comparisonChart.destroy(); comparisonChart = null; }
         return;
    }


    // --- Data Processing for Chart.js ---
    // 1. Get unique, sorted timestamps (labels)
    // Convert timestamps to Date objects for reliable sorting, then format
    const uniqueTimestamps = [...new Set(data.map(item => item.timestamp))];
    const sortedLabels = uniqueTimestamps
                            .map(ts => new Date(ts)) // Convert to Date objects
                            .sort((a, b) => a - b) // Sort Date objects
                            .map(dt => dt.toLocaleString()); // Format for display

    // 2. Create datasets for each selected clan
    const datasets = selectedComparisonClans.map((clanName, index) => {
        // Filter data for the current clan
        const clanDataPoints = data.filter(item => item.clan_name === clanName);

        // Create a map of timestamp string -> points for quick lookup
        const pointMap = new Map();
        clanDataPoints.forEach(item => {
            pointMap.set(new Date(item.timestamp).toLocaleString(), item.current_points);
        });

        // Map data points to the sorted labels, inserting null for missing points
        const points = sortedLabels.map(label => pointMap.get(label) || null);

        // Assign colors dynamically (add more if comparing more than 3 often)
        const colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'];
        const color = colors[index % colors.length];

        return {
            label: clanName,       // Name for the legend
            data: points,          // Array of points (or nulls) matching labels
            borderColor: color,    // Line color
            // backgroundColor: color + '33', // Optional fill color (e.g., 'rgba(255, 99, 132, 0.2)')
            fill: false,           // Don't fill area under line
            tension: 0.1           // Slightly smooth the line
        };
    });

    console.log("Chart labels:", sortedLabels);
    console.log("Chart datasets:", datasets);

    // --- Create or Update Chart ---
    if (comparisonChart) {
        // If chart instance exists, update its data and redraw
        comparisonChart.data.labels = sortedLabels;
        comparisonChart.data.datasets = datasets;
        comparisonChart.update();
        console.log("Chart updated.");
    } else {
        // If chart doesn't exist, create a new one
        comparisonChart = new Chart(ctx, {
            type: 'line', // Line chart
            data: {
                labels: sortedLabels,
                datasets: datasets
            },
            options: {
                responsive: true,              // Make chart responsive
                maintainAspectRatio: false,      // Allow chart to fill container height
                scales: {
                    x: {
                        title: { display: true, text: 'Time' },
                        ticks: { autoSkip: true, maxTicksLimit: 15 } // Limit labels shown
                    },
                    y: {
                        title: { display: true, text: 'Points' },
                        beginAtZero: false // Start Y-axis near lowest value
                    }
                },
                plugins: {
                    title: { display: true, text: 'Clan Points Comparison' },
                    legend: { display: true, position: 'top' } // Show legend
                },
                interaction: { // For tooltips
                     intersect: false,
                     mode: 'index',
                },
            }
        });
        console.log("Chart created.");
    }
}

// --- ** NEW function definition to update header ** ---
function updateGainHeader(periodMinutes) {
     if (gainHeaderElement) { // Check if element exists
          gainHeaderElement.textContent = formatTimePeriodHeader(periodMinutes);
     }
}

// --- Modify initializeApp() to remove collapsing listener ---
async function initializeApp() {
    console.log("Initializing app...");

    // First, populate the battle selector and wait for it
    await populateBattleSelector();

    // Restore target rank from localStorage if present
    const savedTargetRank = localStorage.getItem('selectedTargetRank');
    if (savedTargetRank) {
        currentTargetRank = parseInt(savedTargetRank, 10);
        targetRankInput.value = currentTargetRank;
    }

    // Set initial state from controls
    timePeriodRadios.forEach(radio => { if (radio.checked) currentTimePeriod = parseInt(radio.value, 10); });
    forecastPeriodRadios.forEach(radio => { if (radio.checked) currentForecastPeriod = parseInt(radio.value, 10); });
    currentTargetClan = targetClanSelect.value;
    currentTargetRank = parseInt(targetRankInput.value, 10) || 1;
    targetRankInput.value = currentTargetRank;

    // Add battle selector event listener
    const battleSelect = document.getElementById('battle-select');
    if (battleSelect) {
        battleSelect.addEventListener('change', (event) => {
            currentBattleId = event.target.value;
            console.log(`Battle ID changed to: ${currentBattleId}`);
            // Refresh data when battle changes
            fetchDashboardData();
            if (currentTargetClan) {
                fetchReachTargetData();
            }
        });
    }

    // Set initial comparison time period state
    comparisonTimePeriodRadios.forEach(radio => {
         if (radio.checked) {
             currentComparisonTimePeriod = parseInt(radio.value, 10);
         }
    });
    console.log(`Initial state: timePeriod=<span class="math-inline">\{currentTimePeriod\}, forecastPeriod\=</span>{currentForecastPeriod}, targetClan='<span class="math-inline">\{currentTargetClan\}', targetRank\=</span>{currentTargetRank}, comparisonPeriod=${currentComparisonTimePeriod}`);

    // Update Header on Initial Load
    updateGainHeader(currentTimePeriod);

    // --- ** NEW: Add event listener to the Controls heading ** ---
    // collapsing via header not needed in modal mode
    // controlsHeading click listener removed

    // --- Removed live-change listeners; batch apply via modal ---
    comparisonTimePeriodRadios.forEach(radio => {
         radio.addEventListener('change', (event) => {
              currentComparisonTimePeriod = parseInt(event.target.value, 10);
              console.log(`Comparison Time Period changed to: ${currentComparisonTimePeriod} minutes`);
              // We rely on the button click to update the chart, so no fetch here
         });
     });
    // --- ** END NEW Comparison Listeners ** ---

    // --- Controls Modal Functionality ---
    const openControlsBtn = document.getElementById('open-controls-btn');
    const controlsCancelBtn = document.getElementById('controls-cancel-btn');
    const controlsApplyBtn = document.getElementById('controls-apply-btn');
    const controlsOverlay = document.getElementById('controls-overlay');
    // Save original settings for Cancel
    let modalOriginalSettings = {
        timePeriod: currentTimePeriod,
        forecastPeriod: currentForecastPeriod,
        targetClan: currentTargetClan,
        targetRank: currentTargetRank
    };
    function openControlsModal() {
        // Re-render battle dropdown in case list changed
        if (battleList.length) renderCustomBattleDropdown(battleList);
        // Re-render the tracked clan dropdown with the latest data
        if (clanList.length) renderCustomClanDropdown(clanList);
        // Re-render the target rank dropdown with the current rank
        if (document.getElementById('custom-rank-dropdown')) {
            renderCustomRankDropdown(currentTargetRank);
        }
        modalOriginalSettings = {
            timePeriod: currentTimePeriod,
            forecastPeriod: currentForecastPeriod,
            targetClan: currentTargetClan,
            targetRank: currentTargetRank
        };
        document.getElementById('controls-area').classList.add('modal-visible');
        document.getElementById('controls-overlay').classList.add('modal-visible');
    }
    function closeControlsModal(revert = false) {
        if (revert) {
            // revert UI inputs
            timePeriodRadios.forEach(r => r.checked = (parseInt(r.value, 10) === modalOriginalSettings.timePeriod));
            forecastPeriodRadios.forEach(r => r.checked = (parseInt(r.value, 10) === modalOriginalSettings.forecastPeriod));
            targetClanSelect.value = modalOriginalSettings.targetClan;
            targetRankInput.value = modalOriginalSettings.targetRank;
        }
        document.getElementById('controls-area').classList.remove('modal-visible');
        document.getElementById('controls-overlay').classList.remove('modal-visible');
    }
    openControlsBtn.addEventListener('click', openControlsModal);
    controlsCancelBtn.addEventListener('click', () => closeControlsModal(true));
    controlsOverlay.addEventListener('click', () => closeControlsModal(true));
    controlsApplyBtn.addEventListener('click', () => {
        const prevBattleId = currentBattleId;
        const prevTargetClan = currentTargetClan;
        const prevTargetRank = currentTargetRank;
        const prevTimePeriod = currentTimePeriod;
        const prevForecastPeriod = currentForecastPeriod;
        currentTimePeriod = parseInt(document.querySelector('input[name="time_period"]:checked').value, 10);
        currentForecastPeriod = parseInt(document.querySelector('input[name="forecast_period"]:checked').value, 10);
        currentBattleId = pendingBattleId || currentBattleId;
        currentTargetClan = pendingTargetClan || currentTargetClan;
        currentTargetRank = pendingTargetRank || currentTargetRank;
        // Clear pending values
        pendingBattleId = null;
        pendingTargetClan = null;
        pendingTargetRank = null;
        // --- Persist selected battle to localStorage ---
        localStorage.setItem('selectedBattleId', currentBattleId);
        // --- Persist selected tracked clan to localStorage ---
        localStorage.setItem('selectedClan', currentTargetClan);
        // --- Persist selected target rank to localStorage ---
        localStorage.setItem('selectedTargetRank', currentTargetRank);
        // Only fetch new data if battle, tracked clan, or target rank changed
        if (
            currentBattleId !== prevBattleId ||
            currentTargetClan !== prevTargetClan ||
            currentTargetRank !== prevTargetRank
        ) {
            fetchDashboardData();
            fetchReachTargetData();
        } else {
            // Just re-render the leaderboard and update gain header
            if (lastDashboardData) {
                renderLeaderboardTable(lastDashboardData);
                updateGainHeader(currentTimePeriod);
            }
        }
        closeControlsModal(false);
    });

    // --- Initial data fetch ---
    fetchCountdown();
    setInterval(fetchCountdown, 60000);

    fetchDashboardData(); // Also populates dropdown initially
    setInterval(fetchDashboardData, 120000);

    fetchReachTargetData(true); // Fetch initial reach target data, suppress placeholder

    if (updateComparisonBtn) {
        updateComparisonBtn.addEventListener('click', updateComparisonChart);
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        initializeApp();
        // Start the refresh countdown timer
        setInterval(updateRefreshCountdown, 1000);
    });
} else {
    initializeApp();
    // Start the refresh countdown timer
    setInterval(updateRefreshCountdown, 1000);
}

// Helper to extract image ID from icon string
function getImageId(iconString) {
    if (!iconString) return null;
    const match = iconString.match(/rbxassetid:\/\/(\d+)/);
    return match ? match[1] : null;
}

// Helper to parse time strings like '1h 30m' to minutes
function parseTimeToMinutes(timeStr) {
    if (!timeStr || typeof timeStr !== 'string') return null;
    if (/ended|n\/a|error/i.test(timeStr)) return null;
    let total = 0;
    const hMatch = timeStr.match(/(\d+)h/);
    const mMatch = timeStr.match(/(\d+)m/);
    if (hMatch) total += parseInt(hMatch[1], 10) * 60;
    if (mMatch) total += parseInt(mMatch[1], 10);
    return total > 0 ? total : null;
}

function renderLeaderboardTable(topClans) {
    leaderboardBody.innerHTML = '';
    if (topClans && Array.isArray(topClans) && topClans.length > 0) {
        let trackedClanGain = null;
        if (topClans && Array.isArray(topClans)) {
            const trackedClan = topClans.find(clan => clan.clan_name === currentTargetClan);
            if (trackedClan) {
                const gainField = `gain_${currentTimePeriod}m`;
                trackedClanGain = trackedClan[gainField];
            }
        }
        // Update the gain header to reflect the selected period
        updateGainHeader(currentTimePeriod);
        // --- Calculate gap and time to catch for each clan ---
        const gainField = `gain_${currentTimePeriod}m`; // For Gain column
        const forecastGainField = `gain_${currentForecastPeriod}m`; // For Time to Catch
        for (let i = 0; i < topClans.length; i++) {
            const clan = topClans[i];
            let gap = '';
            let timeToCatch = '';
            if (i > 0) {
                // Calculate gap to the clan above
                const aboveClan = topClans[i - 1];
                if (aboveClan && typeof aboveClan.current_points === 'number' && typeof clan.current_points === 'number') {
                    gap = aboveClan.current_points - clan.current_points;
                    if (gap < 0) gap = 0;
                }
                // Calculate time to catch using forecast period gains
                const currentForecastGain = clan[forecastGainField];
                const aboveForecastGain = aboveClan[forecastGainField];
                if (
                    typeof currentForecastGain === 'number' &&
                    typeof aboveForecastGain === 'number' &&
                    currentForecastGain > aboveForecastGain &&
                    gap > 0 &&
                    currentForecastPeriod > 0
                ) {
                    const gainDifference = currentForecastGain - aboveForecastGain;
                    const minutesToCatch = (gap * currentForecastPeriod) / gainDifference;
                    if (isFinite(minutesToCatch) && minutesToCatch > 0) {
                        // Format as days/hours/minutes if over 24h
                        const days = Math.floor(minutesToCatch / 1440);
                        const hours = Math.floor((minutesToCatch % 1440) / 60);
                        const minutes = Math.round(minutesToCatch % 60);
                        if (days > 0) {
                            let str = `${days}d`;
                            if (hours > 0) str += ` ${hours}h`;
                            if (minutes > 0) str += ` ${minutes}m`;
                            timeToCatch = str;
                        } else if (hours > 0 && minutes > 0) {
                            timeToCatch = `${hours}h ${minutes}m`;
                        } else if (hours > 0) {
                            timeToCatch = `${hours}h`;
                        } else {
                            timeToCatch = `${minutes}m`;
                        }
                    }
                }
            }
            // Render the row
            const row = document.createElement('tr');
            if (clan.clan_name === currentTargetClan) {
                row.classList.add('highlight');
            }
            const gainValue = clan[gainField] !== undefined ? clan[gainField] : null;
            const imageId = getImageId(clan.icon);
            const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';
            
            const warTimeStr = countdownTimerElement.textContent;
            const warTimeMinutes = parseTimeToMinutes(warTimeStr);
            const ttcMinutes = parseTimeToMinutes(timeToCatch);

            let ttcClass = '';
            if (ttcMinutes !== null && warTimeMinutes !== null) {
                ttcClass = ttcMinutes <= warTimeMinutes ? 'ttc-green' : 'ttc-grey';
            }
            
            // --- Add gain-higher-than-tracked class if needed ---
            let gainClass = '';
            if (
                trackedClanGain !== null &&
                gainValue !== null &&
                clan.clan_name !== currentTargetClan &&
                gainValue > trackedClanGain
            ) {
                gainClass = 'gain-higher-than-tracked';
            }
            row.innerHTML = `
                <td>${formatRank(clan.current_rank)}</td>
                <td>${imageUrl ? `<img src="${imageUrl}" class="clan-icon">` : ''}${clan.clan_name || '-'}</td>
                <td>${formatNumber(clan.current_points)}</td>
                <td class="${gainClass}">${gainValue !== null ? formatGain(gainValue) : '-'}</td>
                <td>${gap !== '' && gap !== 0 ? formatNumber(gap) : ''}</td>
                <td${ttcClass ? ` class="${ttcClass}"` : ''}>${timeToCatch}</td>
                <td>${clan.forecast_rank !== undefined ? formatRank(clan.forecast_rank) : '-'}</td>
            `;
            leaderboardBody.appendChild(row);
        }
        lastUpdatedElement.textContent = new Date().toLocaleTimeString();
    } else {
        leaderboardBody.innerHTML = '<tr><td colspan="7">No clan data available.</td></tr>';
        lastUpdatedElement.textContent = new Date().toLocaleTimeString();
    }
}

// --- Helper Functions ---
function updateRefreshCountdown() {
    if (!nextRefreshTime) return;
    
    const now = new Date();
    const timeUntilRefresh = Math.max(0, nextRefreshTime - now);
    const seconds = Math.floor(timeUntilRefresh / 1000);
    
    // Format the countdown
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    const countdownStr = `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
    
    // Update the last updated text with countdown
    const lastUpdatedTime = lastUpdatedElement.getAttribute('data-last-updated') || new Date().toLocaleTimeString();
    lastUpdatedElement.textContent = `${lastUpdatedTime} (Next refresh in ${countdownStr})`;
}