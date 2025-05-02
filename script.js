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
let currentBattleId = localStorage.getItem('selectedBattleId') || 'PixelChickBattle'; // Default to stored or PixelChickBattle
let comparisonChart = null; // Variable to hold the chart instance
let selectedComparisonClans = []; // Array to hold selected clan names
let currentComparisonTimePeriod = 60; // Default comparison period
let controlsExpanded = true; // Initial state of controls
let clanList = []; // Global clan list for dropdown and other uses

// --- API Base URL ---
const API_BASE_URL = "https://clan-dashboard-api.onrender.com"; // Your Render URL

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

        // Save the selected battle ID to localStorage
        localStorage.setItem('selectedBattleId', currentBattleId);

        console.log(`Populated battle selector with ${battles.length} battles. Current battle: ${currentBattleId}`);
    } catch (error) {
        console.error("Error populating battle selector:", error);
        battleSelect.innerHTML = '<option value="">Error loading battles</option>';
    }
}

// --- Fetch Countdown Data ---
async function fetchCountdown() {
    // console.log("Fetching countdown..."); // Reduce logging
    try {
        const response = await fetch(`${API_BASE_URL}/api/countdown`);
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
// --- ** END NEW Helper ** ---


// --- Fetch and Display "Reach Target" Data ---
async function fetchReachTargetData() {
    if (!currentTargetClan) {
        reachTargetDisplay.innerHTML = '<p>Select a Clan to Track in Controls</p>';
        return;
    }
    console.log(`Fetching reach target data for ${currentTargetClan} to rank ${currentTargetRank} (forecast period ${currentForecastPeriod}m)...`);
    reachTargetDisplay.innerHTML = '<p>Calculating...</p>';

    const url = `${API_BASE_URL}/api/clan_reach_target?clan_name=${encodeURIComponent(currentTargetClan)}&target_rank=${currentTargetRank}&forecast_period=${currentForecastPeriod}`;

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
             else { resultText = `Needs **${formatNumber(points)}** extra pts/hr for rank ${currentTargetRank}`; }
        }
         reachTargetDisplay.innerHTML = `<p><strong>${currentTargetClan}</strong>: ${resultText}</p>`;

    } catch (error) {
         console.error("Error fetching reach target data:", error);
         reachTargetDisplay.innerHTML = `<p>Error calculating for ${currentTargetClan}: ${error.message}</p>`;
    }
}


// --- Fetch Dashboard Data (and update controls/highlighting) ---
async function fetchDashboardData() {
    console.log(`Fetching dashboard data with timePeriod=${currentTimePeriod}, forecastPeriod=${currentForecastPeriod}...`);
    const url = `${API_BASE_URL}/api/dashboard?time_period=${currentTimePeriod}&forecast_period=${currentForecastPeriod}`;
    leaderboardBody.innerHTML = '<tr><td colspan="7">Loading...</td></tr>'; // Show loading state
  
    try {
      const response = await fetch(url);
      if (!response.ok) {
        // Attempt to parse error detail from response if not ok
        let errorDetail = `HTTP error! status: ${response.status}`;
        try {
          const errorData = await response.json();
          errorDetail += ` - ${JSON.stringify(errorData.detail || errorData)}`; // Use .detail if available
        } catch (jsonError) { /* Ignore if response wasn't JSON */ }
        throw new Error(errorDetail);
      }
      clanList = await response.json();
      console.log("Dashboard data received (first few):", clanList.slice(0, 3));
  
      leaderboardBody.innerHTML = ''; // Clear loading/previous rows
  
      if (clanList && Array.isArray(clanList) && clanList.length > 0) {
        // --- Populate Target Clan Dropdown (Existing logic) ---
        const previousSelectedClan = targetClanSelect.value;
        targetClanSelect.innerHTML = '<option value="">-- Select Clan --</option>'; // Clear and add default
        clanList.forEach(clan => {
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
            fetchReachTargetData();
        } else if (!previousSelectedClan) {
            // Only set default on very first load
            const nongOption = Array.from(targetClanSelect.options).find(opt => opt.value === "NONG");
            if (nongOption) {
                targetClanSelect.value = "NONG";
                currentTargetClan = "NONG";
                localStorage.setItem('selectedClan', "NONG");
                fetchReachTargetData();
            } else if (targetClanSelect.options.length > 1) {
                // Use the first available clan (not the placeholder)
                targetClanSelect.selectedIndex = 1;
                currentTargetClan = targetClanSelect.value;
                localStorage.setItem('selectedClan', currentTargetClan);
                fetchReachTargetData();
            }
        } else {
            // Restore previous selection
            targetClanSelect.value = previousSelectedClan;
            currentTargetClan = previousSelectedClan;
        }
        // --- End Dropdown Population ---
  
        // --- Populate All Clan Pills ---
        if (allClanPillsContainer) {
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
        } else {
          console.error("All clan pills container not found!");
        }
        // --- End Populate All Clan Pills ---
  
        // --- Populate table rows (Existing logic) ---
        let trackedClanGain = null;
        if (clanList && Array.isArray(clanList)) {
            const trackedClan = clanList.find(clan => clan.clan_name === currentTargetClan);
            if (trackedClan) trackedClanGain = trackedClan.x_minute_gain;
        }
  
        clanList.forEach(clan => {
          const row = document.createElement('tr');
          if (clan.clan_name === currentTargetClan) { // Add highlighting
            row.classList.add('highlight');
          }
          // Get image ID and URL
          const imageId = getImageId(clan.icon);
          const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';

          const ttcStr = formatTimeOrNA(clan.time_to_catch);
          const ttcMins = parseTimeToMinutes(ttcStr);
          const warEnded = /ended|n\/a|error/i.test(countdownTimerElement.textContent);
          let ttcColor = '';
          if (ttcMins !== null) {
              if (warEnded) {
                  ttcColor = 'red';
              } else {
                  const warMins = parseTimeToMinutes(countdownTimerElement.textContent);
                  if (warMins !== null) {
                      ttcColor = ttcMins <= warMins ? 'green' : 'red';
                  }
              }
          }

          row.innerHTML = `
              <td>${formatRank(clan.current_rank)}</td>
              <td>
                  ${imageUrl ? `<img src="${imageUrl}" alt="icon" class="clan-icon">` : ''}
                  ${clan.clan_name || '-'}
              </td>
              <td>${formatNumber(clan.current_points)}</td>
              <td>${
                  (clan.clan_name !== currentTargetClan && trackedClanGain !== null && clan.x_minute_gain > trackedClanGain)
                      ? `<span style='color: red'>${formatGain(clan.x_minute_gain)}</span>`
                      : formatGain(clan.x_minute_gain)
              }</td>
              <td>${formatNumber(clan.gap)}</td>
              <td>${ttcColor && ttcStr !== '-' ? `<span style='color: ${ttcColor}'>${ttcStr}</span>` : ttcStr}</td>
              <td>${formatRank(clan.forecast)}</td>
            `;
          leaderboardBody.appendChild(row);
        });
        // Update Last Updated timestamp
        lastUpdatedElement.textContent = new Date().toLocaleTimeString();
  
      } else {
        // Handle empty data case
        console.warn("Received empty or invalid clan list:", clanList);
        leaderboardBody.innerHTML = '<tr><td colspan="7">No clan data available.</td></tr>';
        if (comparisonClanListDiv) comparisonClanListDiv.innerHTML = '(No clans to select)'; // Update placeholder if empty
        lastUpdatedElement.textContent = new Date().toLocaleTimeString();
      }
  
    } catch (error) {
      console.error("Error fetching or processing dashboard data:", error);
      leaderboardBody.innerHTML = `<tr><td colspan="7">Error loading data. Check console. (${error.message})</td></tr>`;
      if (comparisonClanListDiv) comparisonClanListDiv.innerHTML = '(Error loading clans)'; // Update placeholder on error
      lastUpdatedElement.textContent = new Date().toLocaleTimeString();
    }

    // 1. Hide the old select and add a new div for the custom dropdown
    console.log('Attempting to hide old select and insert custom dropdown...');
    if (targetClanSelect) {
        targetClanSelect.style.display = 'none';
        console.log('targetClanSelect found and hidden.');
    } else {
        console.warn('targetClanSelect not found!');
    }
    let customDropdown = document.getElementById('custom-clan-dropdown');
    if (!customDropdown) {
        customDropdown = document.createElement('div');
        customDropdown.id = 'custom-clan-dropdown';
        customDropdown.className = 'custom-clan-dropdown';
        if (targetClanSelect && targetClanSelect.parentNode) {
            targetClanSelect.parentNode.insertBefore(customDropdown, targetClanSelect);
            console.log('Inserted custom dropdown into DOM.');
        } else {
            console.warn('Could not insert custom dropdown: targetClanSelect or its parentNode is missing.');
        }
    } else {
        console.log('Custom dropdown already exists in DOM.');
    }

    // Helper to render the custom dropdown
    console.log('Calling renderCustomClanDropdown with clanList:', clanList);
    renderCustomClanDropdown(clanList);

    // Hide the old number input and add a new div for the custom rank dropdown
    if (targetRankInput) targetRankInput.style.display = 'none';
    let customRankDropdown = document.getElementById('custom-rank-dropdown');
    if (!customRankDropdown) {
        customRankDropdown = document.createElement('div');
        customRankDropdown.id = 'custom-rank-dropdown';
        customRankDropdown.className = 'custom-rank-dropdown';
        targetRankInput.parentNode.insertBefore(customRankDropdown, targetRankInput);
    }

    function renderCustomRankDropdown(selectedRank = 1) {
        const dropdown = document.getElementById('custom-rank-dropdown');
        dropdown.innerHTML = '';
        const selected = document.createElement('div');
        selected.className = 'custom-rank-selected';
        selected.innerHTML = `
            <span>${selectedRank}</span>
            <span class="dropdown-arrow">&#9662;</span>
        `;
        dropdown.appendChild(selected);

        const list = document.createElement('ul');
        list.className = 'rank-dropdown-list';
        for (let i = 1; i <= 20; i++) {
            const item = document.createElement('li');
            item.className = 'rank-dropdown-item';
            item.textContent = i;
            item.onclick = () => {
                currentTargetRank = i;
                renderCustomRankDropdown(i);
                fetchReachTargetData();
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

    // In fetchDashboardData or initializeApp, after rendering the controls, call:
    renderCustomRankDropdown(currentTargetRank);
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
    const selected = document.createElement('div');
    selected.className = 'custom-clan-selected';
    let selectedClan = clanList.find(c => c.clan_name === currentTargetClan) || clanList[0];
    const imageId = getImageId(selectedClan.icon);
    const imageUrl = imageId ? `https://ps99.biggamesapi.io/image/${imageId}` : '';
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
        item.onclick = () => {
            currentTargetClan = clan.clan_name;
            localStorage.setItem('selectedClan', currentTargetClan); // Save to localStorage
            renderCustomClanDropdown(clanList);
            fetchReachTargetData();
            fetchDashboardData();
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
    let comparisonUrl = `${API_BASE_URL}/api/clan_comparison?time_period=${currentComparisonTimePeriod}`;
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

// --- ** NEW function to toggle controls visibility ** ---
function toggleControls() {
    controlsExpanded = !controlsExpanded;
    controlsArea.classList.toggle('collapsed');
    console.log(`Controls section is now ${controlsExpanded ? 'expanded' : 'collapsed'}`);
  }

// Modify initializeApp() to include the battle selector population
async function initializeApp() {
    console.log("Initializing app...");

    // First, populate the battle selector and wait for it
    await populateBattleSelector();

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
     if (controlsHeading) {
    controlsHeading.addEventListener('click', toggleControls);
        } else {
    console.error("Controls heading (h2 inside #controls-area) not found!");
        }

    // --- Add listeners for Dashboard Controls ---
    timePeriodRadios.forEach(radio => {
        radio.addEventListener('change', (event) => {
            currentTimePeriod = parseInt(event.target.value, 10);
            console.log(`Gain Period changed to: ${currentTimePeriod} minutes`);
            updateGainHeader(currentTimePeriod); // Update header on change
            fetchDashboardData();
        });
    });

    forecastPeriodRadios.forEach(radio => {
        radio.addEventListener('change', (event) => {
            currentForecastPeriod = parseInt(event.target.value, 10);
            console.log(`Forecast Period changed to: ${currentForecastPeriod} minutes`);
            fetchDashboardData();
            fetchReachTargetData();
        });
    });

    targetClanSelect.addEventListener('change', (event) => {
        currentTargetClan = event.target.value;
        console.log(`Target Clan changed to: ${currentTargetClan}`);
        fetchDashboardData(); // Refreshes table and applies highlight
        fetchReachTargetData();
    });

    targetRankInput.addEventListener('change', (event) => {
        let rankValue = parseInt(event.target.value, 10);
        if (isNaN(rankValue) || rankValue < 1) { rankValue = 1; }
        else if (rankValue > 250) { rankValue = 250; }
        currentTargetRank = rankValue;
        targetRankInput.value = currentTargetRank;
        console.log(`Target Rank changed to: ${currentTargetRank}`);
        fetchReachTargetData();
    });
    // --- End Dashboard Control Listeners ---


    // --- ** NEW: Add Listeners for Comparison Controls ** ---
    if (updateComparisonBtn) {
        console.log("Attaching click listener to Update Comparison button."); // Verify attachment
        updateComparisonBtn.addEventListener('click', updateComparisonChart);
    } else {
        console.error("Update Comparison Button not found!");
    }

    comparisonTimePeriodRadios.forEach(radio => {
         radio.addEventListener('change', (event) => {
              currentComparisonTimePeriod = parseInt(event.target.value, 10);
              console.log(`Comparison Time Period changed to: ${currentComparisonTimePeriod} minutes`);
              // We rely on the button click to update the chart, so no fetch here
         });
     });
    // --- ** END NEW Comparison Listeners ** ---


    // --- Initial data fetch ---
    fetchCountdown();
    setInterval(fetchCountdown, 60000);

    fetchDashboardData(); // Also populates dropdown initially
    setInterval(fetchDashboardData, 120000);

    fetchReachTargetData(); // Fetch initial reach target data
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        initializeApp();
    });
} else {
    initializeApp();
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
