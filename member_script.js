// --- Constants ---
const UPDATE_INTERVAL = 120000; // 2 minutes in milliseconds
const LOADING_PLACEHOLDER = '<span class="loading-spinner"></span>';

// const API_BASE_URL = "http://127.0.0.1:8000"; // Local combined API server
const API_BASE_URL = "https://clan-dashboard-api.onrender.com"; // Production Render server

// --- DOM Elements ---
const clanSelect = document.getElementById('clan-select');
const battleSelect = document.getElementById('battle-select');
const memberTableBody = document.getElementById('member-table-body');
const lastUpdatedElement = document.getElementById('last-updated');
const pointsGainedPeriodElement = document.getElementById('points-gained-period');
const timePeriodSelect = document.querySelectorAll('input[name="timePeriod"]');
const avgPointsElement = document.getElementById('avg-points');
const clanPointsHourElement = document.getElementById('clan-points-hour');
const avgPointsHourElement = document.getElementById('avg-points-hour');
const avgUptimeElement = document.getElementById('avg-uptime');
const activeCountElement = document.getElementById('active-count');
const uptimeWindowInputs = document.querySelectorAll('input[name="uptimeWindow"]');

// --- State Management ---
let currentClan = null;
let currentBattle = null;
let cachedMemberData = null;
let cachedHistoryData = null;
let selectedUptimeWindow = 2;
let selectedTimePeriod = 60;  // Default to 60 minutes (1h)
let uptimeCache = {};
let isInitialLoad = true;
let lastFullHistoryFetch = null;
let lastUptimeValues = new Map();
let currentMembers = [];
let memberHistory = new Map();
let lastUptimeCalculation = null;  // Track when we last calculated uptimes
let currentSortColumn = 'points'; // Default sort by points
let currentSortDirection = 'desc'; // Default sort direction

// Add clan persistence
function saveCurrentClan() {
    localStorage.setItem('selectedClan', currentClan);
}

function loadSavedClan() {
    return localStorage.getItem('selectedClan') || 'NONG';  // Default to NONG if no saved clan
}

// --- Formatting Helpers ---
function formatNumber(num) {
    return num.toLocaleString();
}

function formatTimePeriod(minutes) {
    if (minutes >= 60) {
        return `${minutes/60}h`;
    }
    return `${minutes}m`;
}

function formatPercentage(num) {
    return num.toFixed(1) + '%';  // num is already 0-100
}

function formatPointChange(change) {
    if (change === null || change === undefined) return '-';
    return formatNumber(change);
}

function formatInactiveTime(minutes) {
    if (minutes === 0) return '';
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    if (hours === 0) return `${mins}m`;
    return `${hours}h ${mins}m`;
}

// --- Data Processing Functions ---
function calculatePointGains(memberData, historyData) {
    if (!historyData?.history?.length || !memberData?.members?.length) {
        return new Map();
    }

    // Get the sorted history (already ordered newest to oldest)
    const sortedHistory = historyData.history;
    
    // Calculate how many entries we need (2 minutes per entry)
    const entriesNeeded = Math.floor(selectedTimePeriod / 2);
    
    // Since data is newest to oldest, we want to look back from index 0
    // But don't go beyond the available data
    const targetIndex = Math.min(entriesNeeded, sortedHistory.length - 1);
    
    // Get the points at start of period (older) and end of period (newer)
    const startRecord = sortedHistory[targetIndex];  // Older
    const endRecord = sortedHistory[0];             // Newer (most recent)

    const pointGains = new Map();

    // Calculate gains from start to end
    endRecord.members.forEach(member => {
        if (!member.UserID) return;
        const startPoints = startRecord.members.find(m => m.UserID === member.UserID)?.points || member.points;
        pointGains.set(member.UserID, member.points - startPoints);
    });

    return pointGains;
}

// Add a helper to fetch and populate all battle IDs
async function populateBattleSelector() {
    try {
        console.log("Fetching battle IDs for member dashboard...");
        const response = await fetch(`${API_BASE_URL}/api/clan/api/battle_ids`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const battles = await response.json();

        // Clear existing options
        battleSelect.innerHTML = '';

        // Load saved battle from localStorage
        const savedBattle = localStorage.getItem('selectedBattle');

        // Populate dropdown
        battles.forEach(({ battle_id }) => {
            const option = document.createElement('option');
            option.value = battle_id;
            option.textContent = battle_id;
            battleSelect.appendChild(option);
        });

        // Select saved or first battle
        const defaultBattle = savedBattle && battles.some(b => b.battle_id === savedBattle)
            ? savedBattle
            : (battles[0]?.battle_id || '');
        if (defaultBattle) {
            battleSelect.value = defaultBattle;
            currentBattle = defaultBattle;
            localStorage.setItem('selectedBattle', defaultBattle);
        }
    } catch (error) {
        console.error("Error populating battle selector:", error);
        battleSelect.innerHTML = '<option value="">Error loading battles</option>';
    }
}

// Ensure updateBattleSelect does not clear the full dropdown
function updateBattleSelect(memberData) {
    if (!memberData?.battle_id) {
        console.error('No battle_id found in member data');
        return;
    }
    const id = memberData.battle_id;
    // Add option if missing
    if (![...battleSelect.options].some(opt => opt.value === id)) {
        const opt = document.createElement('option');
        opt.value = id;
        opt.textContent = id;
        battleSelect.appendChild(opt);
    }
    // Select current battle
    battleSelect.value = id;
    currentBattle = id;
    console.log(`Battle selector set to battle_id: ${id}`);
}

// Add CSS for loading overlay
const style = document.createElement('style');
style.textContent = `
.loading-spinner {
    display: inline-block;
    width: 16px;
    height: 16px;
    border: 2px solid #f3f3f3;
    border-top: 2px solid #3498db;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    vertical-align: middle;
}

.loading-overlay {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    justify-content: center;
    align-items: center;
    z-index: 9999;
    opacity: 0;
    visibility: hidden;
    transition: opacity 0.3s, visibility 0.3s;
}

.loading-overlay.active {
    opacity: 1;
    visibility: visible;
}

.loading-content {
    background: white;
    padding: 20px;
    border-radius: 8px;
    text-align: center;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
}

.loading-content .loading-spinner {
    width: 40px;
    height: 40px;
    margin-bottom: 10px;
}

@keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
}
`;
document.head.appendChild(style);

// Add loading overlay HTML
const loadingOverlay = document.createElement('div');
loadingOverlay.className = 'loading-overlay';
loadingOverlay.innerHTML = `
    <div class="loading-content">
        <div class="loading-spinner"></div>
        <div>Loading battle data...</div>
    </div>
`;
document.body.appendChild(loadingOverlay);

// Function to show/hide loading overlay
function setLoading(show) {
    loadingOverlay.classList.toggle('active', show);
}

// Add battle change handler
async function handleBattleChange() {
    const selectedBattle = battleSelect.value;
    if (!selectedBattle) return;
    
    console.log(`Battle changed to: ${selectedBattle}`);
    currentBattle = selectedBattle;
    
    try {
        setLoading(true);  // Show loading overlay
        
        // Fetch full history for this battle and use the newest record's members
        console.log('Fetching battle history to build roster...');
        const historyData = await fetchMemberHistory(currentClan, selectedBattle);
        if (!historyData?.history?.length) {
            throw new Error('No history available for this battle');
        }
        cachedHistoryData = historyData;
        // historyData.history is sorted newest-to-oldest, so take [0]
        const battleMembers = historyData.history[0].members;
        console.log(`Loaded ${battleMembers.length} members from history for battle ${selectedBattle}`);
        
        // Clear all uptime caches when battle changes
        uptimeCache = {};
        lastUptimeValues = new Map();
        lastUptimeCalculation = null;  // Force recalculation for new battle
        
        // Calculate uptime for each member
        console.log('Calculating uptimes for members...');
        battleMembers.forEach(member => {
            if (!member.UserID) return;
            const uptime = calculateMemberUptime(historyData, member.UserID, selectedUptimeWindow);
            console.log(`Uptime for ${member.username}: ${uptime}%`);
            lastUptimeValues.set(member.UserID, uptime);
        });
        
        // Calculate point gains for the selected time period
        const selectedPeriod = parseInt(timePeriodSelect?.value) || selectedTimePeriod;
        pointsGainedPeriodElement.textContent = formatTimePeriod(selectedPeriod);
        const pointGains = calculatePointGains({ ...cachedMemberData, members: battleMembers }, historyData);
        
        // Update the display
        updateStats({ ...cachedMemberData, members: battleMembers });
        renderMemberTable({ ...cachedMemberData, members: battleMembers }, pointGains);
        
        // Calculate and update other stats using battle members
        const stats = await calculateStats({ ...cachedMemberData, members: battleMembers }, historyData, selectedUptimeWindow);
        updateStatsDisplay(stats);
        
        lastUpdatedElement.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
    } catch (error) {
        console.error('Error in handleBattleChange:', error);
        console.error('Stack trace:', error.stack);
        memberTableBody.innerHTML = `<tr><td colspan="5" class="text-center">Error loading battle data: ${error.message}</td></tr>`;
    } finally {
        setLoading(false);  // Hide loading overlay
    }
}

// --- Data Fetching Functions ---
async function fetchMemberData(clanName) {
    try {
        console.log(`Fetching member data for clan: ${clanName}`);
        const response = await fetch(`${API_BASE_URL}/api/member/member-tracking/${clanName}?battle_id=${currentBattle}`);
        if (!response.ok) {
            console.error(`HTTP error! status: ${response.status}`);
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log('Received member data:', {
            status: data.status,
            clan_name: data.clan_name,
            battle_id: data.battle_id,
            member_count: data.members?.length || 0
        });
        return data;
    } catch (error) {
        console.error(`Error fetching member data for ${clanName}:`, error);
        return null;
    }
}

async function fetchRecentHistory(clanName) {
    const startTime = performance.now();
    try {
        console.log(`[Timing] Starting recent history fetch for ${clanName}`);
        const response = await fetch(`${API_BASE_URL}/api/member/member-history/${clanName}/recent?hours=24&battle_id=${currentBattle}`);
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log(`[Timing] Records received: ${data.history?.length || 0}`);
        
        if (!data || !data.history || data.history.length === 0) {
            throw new Error('No history data received');
        }

        const endTime = performance.now();
        console.log(`[Timing] History data fetch took ${(endTime - startTime).toFixed(1)}ms`);
        return data;
    } catch (error) {
        console.error(`Error fetching history for ${clanName}:`, error);
        throw error; // Let the caller handle the error
    }
}

async function fetchMemberHistory(clanName, battleId = null) {
    try {
        console.log(`Fetching history for clan: ${clanName}` + (battleId ? ` and battle: ${battleId}` : ''));
        const url = `${API_BASE_URL}/api/member/member-history/${clanName}` + (battleId ? `?battle_id=${battleId}` : '');
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        console.log('History data received:', {
            recordCount: data.history.length,
            battleId: data.battle_id,
            timeSpan: data.history.length > 0 ? 
                `${new Date(data.history[0].timestamp).toISOString()} to ${new Date(data.history[data.history.length-1].timestamp).toISOString()}` : 
                'No data'
        });
        return data;
    } catch (error) {
        console.error(`Error fetching history for ${clanName}:`, error);
        return null;
    }
}

// --- Rendering Functions ---
function updateStats(memberData) {
    if (!memberData) {
        avgPointsElement.textContent = '-';
        return;
    }

    const totalMembers = memberData.members.length;
    const totalPoints = memberData.total_points;
    const avgPoints = totalMembers > 0 ? Math.round(totalPoints / totalMembers) : 0;

    avgPointsElement.textContent = formatNumber(avgPoints);
}

function sortMembers(members, column, pointGains) {
    return [...members].sort((a, b) => {
        let comparison = 0;
        switch(column) {
            case 'points':
                comparison = b.points - a.points;
                break;
            case 'inactive':
                const inactiveA = calculateInactiveTime(cachedHistoryData, a.UserID, selectedUptimeWindow) || 0;
                const inactiveB = calculateInactiveTime(cachedHistoryData, b.UserID, selectedUptimeWindow) || 0;
                comparison = inactiveA - inactiveB;
                break;
            case 'uptime':
                const uptimeA = lastUptimeValues.get(a.UserID) || 0;
                const uptimeB = lastUptimeValues.get(b.UserID) || 0;
                comparison = uptimeB - uptimeA;
                break;
            case 'points_gained':
                const gainA = pointGains.get(a.UserID) || 0;
                const gainB = pointGains.get(b.UserID) || 0;
                comparison = gainB - gainA;
                break;
        }
        return currentSortDirection === 'desc' ? comparison : -comparison;
    });
}

function calculatePointStats(members, pointGains = null) {
    if (!members || members.length === 0) return { mean: 0, stdDev: 0 };
    
    // If pointGains is provided, calculate stats for gains instead of points
    const values = pointGains 
        ? members.map(member => pointGains.get(member.UserID) || 0)
        : members.map(member => member.points);
    
    // Calculate mean
    const sum = values.reduce((acc, val) => acc + val, 0);
    const mean = sum / values.length;
    
    // Calculate standard deviation
    const squareDiffs = values.map(val => {
        const diff = val - mean;
        return diff * diff;
    });
    const avgSquareDiff = squareDiffs.reduce((acc, val) => acc + val, 0) / values.length;
    const stdDev = Math.sqrt(avgSquareDiff);
    
    return { mean, stdDev };
}

function getValueCategory(value, mean, stdDev) {
    if (!stdDev) return ''; // Avoid division by zero
    
    const zScore = (value - mean) / stdDev;
    
    if (zScore > 1) return 'points-exceptional';      // More than 1 std dev above mean
    if (zScore >= -0.5) return 'points-acceptable';   // Between -0.5 and +1 std dev
    if (zScore >= -1) return 'points-underperforming'; // Between -1 and -0.5 std dev
    return 'points-unacceptable';                     // More than 1 std dev below mean
}

function getUptimeCategory(uptimePercentage) {
    if (uptimePercentage >= 98) return 'points-exceptional';
    if (uptimePercentage >= 90) return 'points-acceptable';
    if (uptimePercentage >= 75) return 'points-underperforming';
    return 'points-unacceptable';
}

function renderMemberTable(memberData, pointGains = {}) {
    if (!memberData || !memberData.members) {
        memberTableBody.innerHTML = '<tr><td colspan="6" class="text-center">No member data available</td></tr>';
        return;
    }

    // Calculate point statistics for both columns
    const pointStats = calculatePointStats(memberData.members);
    const gainStats = calculatePointStats(memberData.members, pointGains);

    // Sort members based on current sort column
    const sortedMembers = sortMembers(memberData.members, currentSortColumn, pointGains);

    memberTableBody.innerHTML = sortedMembers.map((member, index) => {
        const pointGain = pointGains.get(member.UserID);
        const uptimeValue = lastUptimeValues.get(member.UserID);
        const inactiveTime = calculateInactiveTime(cachedHistoryData, member.UserID, selectedUptimeWindow);
        
        // Calculate rank based on points (always show points-based rank)
        const rank = memberData.members
            .sort((a, b) => b.points - a.points)
            .findIndex(m => m.UserID === member.UserID) + 1;

        // Get category classes for points and gains
        const pointsClass = getValueCategory(member.points, pointStats.mean, pointStats.stdDev);
        const gainClass = pointGain !== undefined ? getValueCategory(pointGain, gainStats.mean, gainStats.stdDev) : '';
        
        // Get category class for uptime
        const uptimeClass = uptimeValue !== undefined ? getUptimeCategory(uptimeValue) : '';
       
        return `
            <tr>
                <td>${rank}</td>
                <td><a href="member_details.html?clan=${currentClan}&userId=${member.UserID}&battle=${currentBattle}" target="_blank" rel="noopener noreferrer">${member.username}</a></td>
                <td class="${pointsClass}">${formatNumber(member.points)}</td>
                <td class="inactive-time-${member.UserID}">${inactiveTime !== null ? formatInactiveTime(inactiveTime) : LOADING_PLACEHOLDER}</td>
                <td class="uptime-cell-${member.UserID} ${uptimeClass}">${uptimeValue !== undefined ? formatPercentage(uptimeValue) : LOADING_PLACEHOLDER}</td>
                <td class="${gainClass}">${pointGain !== undefined ? formatPointChange(pointGain) : LOADING_PLACEHOLDER}</td>
            </tr>
        `;
    }).join('');

    // Update sort indicators on table headers
    document.querySelectorAll('.member-dashboard .table-wrapper table th').forEach(th => {
        const headerText = th.textContent.toLowerCase().split('(')[0].trim();
        let column;
        
        // Map column headers to sort keys
        switch(headerText) {
            case 'rank':
            case 'points':
                column = 'points';
                break;
            case 'inactive time':
                column = 'inactive';
                break;
            case 'uptime %':
                column = 'uptime';
                break;
            case 'points gained':
                column = 'points_gained';
                break;
            default:
                return; // Don't sort other columns
        }

        // Remove existing click listener
        const clone = th.cloneNode(true);
        
        // Preserve the points gained period text if this is the points gained column
        if (headerText === 'points gained') {
            const periodSpan = clone.querySelector('#points-gained-period');
            if (periodSpan) {
                periodSpan.textContent = pointsGainedPeriodElement.textContent;
            }
        }

        th.parentNode.replaceChild(clone, th);

        // Set sort indicator
        if (column === currentSortColumn) {
            clone.setAttribute('data-sort', currentSortDirection);
        } else {
            clone.setAttribute('data-sort', '');
        }

        // Add click handler
        clone.addEventListener('click', () => {
            if (column === currentSortColumn) {
                currentSortDirection = currentSortDirection === 'desc' ? 'asc' : 'desc';
            } else {
                currentSortColumn = column;
                currentSortDirection = 'desc';
            }

            // Calculate point gains before rendering
            const pointGains = calculatePointGains(cachedMemberData, cachedHistoryData);
            renderMemberTable(cachedMemberData, pointGains);
        });
    });
}

// --- Uptime Calculation Functions ---
function shouldRecalculateUptime() {
    if (!lastUptimeCalculation) return true;
    
    // Recalculate if it's been more than an hour
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    return lastUptimeCalculation < oneHourAgo;
}

function calculateMemberUptime(historyData, userId, windowMinutes) {
    // Check cache first
    const cacheKey = `${userId}_${windowMinutes}`;
    if (uptimeCache[cacheKey] && !shouldRecalculateUptime()) {
        console.log(`Using cached uptime for ${userId} with window ${windowMinutes}m`);
        return uptimeCache[cacheKey];
    }

    if (!historyData?.history?.length) {
        return 0;
    }

    const history = historyData.history;
    
    // Extract points history for this user
    const userPoints = [];
    for (const record of history) {
        const member = record.members.find(m => m.UserID === userId);
        if (member) {
            userPoints.push(member.points);
        }
    }

    if (userPoints.length < 2) {
        return 0;
    }

    let activeCount = 0;
    let totalCount = 0;

    // Handle different window sizes
    if (windowMinutes === 2) {
        // For 2m windows, compare consecutive entries
        for (let i = 0; i < userPoints.length - 1; i++) {
            if (userPoints[i] !== userPoints[i + 1]) {
                activeCount++;
            }
            totalCount++;
        }
    } else {
        // For 6m and 10m windows, group into blocks
        const blockSize = windowMinutes === 6 ? 3 : 5; // 3 entries for 6m, 5 entries for 10m
        
        // Process complete blocks only
        for (let i = 0; i <= userPoints.length - blockSize; i += blockSize) {
            const blockStart = userPoints[i];
            const blockEnd = userPoints[i + blockSize - 1];
            
            if (blockStart !== blockEnd) {
                activeCount++;
            }
            totalCount++;
        }
    }

    const uptime = totalCount > 0 ? (activeCount / totalCount) * 100 : 0;
    
    // Cache the result
    uptimeCache[cacheKey] = uptime;
    lastUptimeCalculation = new Date();
    
    return uptime;
}

// --- Statistics Calculation Functions ---
function calculateStats(memberData, historyData, uptimeWindow) {
    if (!memberData?.members?.length || !historyData?.history?.length) {
        return {
            avgPoints: 0,
            clanPointsHour: 0,
            avgPointsHour: 0,
            avgUptime: 0,
            activeCount: 0
        };
    }

    // Average points (current)
    const totalPoints = memberData.members.reduce((sum, member) => sum + member.points, 0);
    const avgPoints = totalPoints / memberData.members.length;

    // Points in last hour
    const hourAgoData = calculatePointGains(memberData, historyData);
    const clanPointsHour = Array.from(hourAgoData.values()).reduce((sum, gain) => sum + gain, 0);
    const avgPointsHour = clanPointsHour / memberData.members.length;

    // Average uptime
    const uptimes = memberData.members.map(member => 
        calculateMemberUptime(historyData, member.UserID, uptimeWindow)
    );
    const avgUptime = uptimes.reduce((sum, uptime) => sum + uptime, 0) / uptimes.length;

    // Active members count (scored in last window)
    const windowData = calculatePointGains(memberData, historyData);
    const activeCount = Array.from(windowData.values()).filter(gain => gain > 0).length;

    return {
        avgPoints,
        clanPointsHour,
        avgPointsHour,
        avgUptime,
        activeCount
    };
}

// --- Update Functions ---
function updateStatsDisplay(stats) {
    avgPointsElement.textContent = formatNumber(Math.round(stats.avgPoints));
    clanPointsHourElement.textContent = formatNumber(Math.round(stats.clanPointsHour));
    avgPointsHourElement.textContent = formatNumber(Math.round(stats.avgPointsHour));
    avgUptimeElement.textContent = formatPercentage(stats.avgUptime); // Already a percentage
    activeCountElement.textContent = stats.activeCount;
}

// --- Event Handlers ---
async function handleClanChange(isRefresh = false) {
    const selectedClan = clanSelect.value;
    
    if (!selectedClan) {
        currentClan = null;
        currentBattle = null;
        memberTableBody.innerHTML = '<tr><td colspan="5" class="text-center">Select a clan to view members</td></tr>';
        return;
    }

    if (!isRefresh) {
        currentClan = selectedClan;
        saveCurrentClan();  // Save the selected clan
        memberTableBody.innerHTML = '<tr><td colspan="5" class="text-center">Loading...</td></tr>';
    }

    try {
        console.log(`Starting data fetch for clan: ${selectedClan}, isRefresh: ${isRefresh}`);
        
        // Fetch member data
        const memberData = await fetchMemberData(currentClan);
        if (!memberData) {
            console.error('Failed to fetch member data');
            throw new Error('Failed to fetch member data');
        }
        
        console.log('Member data received:', {
            battle_id: memberData.battle_id,
            total_members: memberData.members?.length || 0
        });
        
        // Update battle selector with available battles
        updateBattleSelect(memberData);
        
        cachedMemberData = memberData;

        // Fetch history data for the current battle
        try {
            console.log('Fetching battle-specific history data...');
            const historyData = await fetchMemberHistory(currentClan, memberData.battle_id);
            if (!historyData) {
                throw new Error('Failed to fetch history data');
            }
            cachedHistoryData = historyData;
            
            // Filter members for current battle
            const battleMembers = memberData.members.filter(m => m.battle_id === memberData.battle_id);
            console.log(`Using ${battleMembers.length} members for battle ${memberData.battle_id}`);
            
            // Clear uptime cache when loading new data
            uptimeCache = {};
            
            // Calculate uptime for each member
            console.log('Calculating uptimes for members...');
            battleMembers.forEach(member => {
                if (!member.UserID) return;
                const uptime = calculateMemberUptime(historyData, member.UserID, selectedUptimeWindow);
                console.log(`Uptime for ${member.username}: ${uptime}%`);
                lastUptimeValues.set(member.UserID, uptime); // Store as percentage
            });
            
            // Calculate point gains for the selected time period
            const selectedPeriod = parseInt(timePeriodSelect?.value) || selectedTimePeriod;
            pointsGainedPeriodElement.textContent = formatTimePeriod(selectedPeriod);
            const pointGains = calculatePointGains({ ...memberData, members: battleMembers }, historyData);
            
            // Update the display
            updateStats({ ...memberData, members: battleMembers });
            renderMemberTable({ ...memberData, members: battleMembers }, pointGains);
            
            // Calculate and update other stats using battle members
            const stats = await calculateStats({ ...memberData, members: battleMembers }, historyData, selectedUptimeWindow);
            updateStatsDisplay(stats);
            
            lastUpdatedElement.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
        } catch (error) {
            console.error('Error in handleClanChange:', error);
            console.error('Stack trace:', error.stack);
            memberTableBody.innerHTML = `<tr><td colspan="5" class="text-center">Error loading member data: ${error.message}</td></tr>`;
        }
    } catch (error) {
        console.error('Error in handleClanChange:', error);
        console.error('Stack trace:', error.stack);
        memberTableBody.innerHTML = `<tr><td colspan="5" class="text-center">Error loading member data: ${error.message}</td></tr>`;
    }
}

// Add event listeners
clanSelect.addEventListener('change', () => handleClanChange());
battleSelect.addEventListener('change', handleBattleChange);

// Time period selector for points gained
timePeriodSelect.forEach(input => {
    input.addEventListener('change', (e) => {
        selectedTimePeriod = parseInt(e.target.value);
        pointsGainedPeriodElement.textContent = formatTimePeriod(selectedTimePeriod);
        
        if (currentClan && cachedMemberData && cachedHistoryData) {
            const battleMembers = cachedMemberData.members.filter(m => m.battle_id === currentBattle);
            const pointGains = calculatePointGains({ ...cachedMemberData, members: battleMembers }, cachedHistoryData);
            renderMemberTable({ ...cachedMemberData, members: battleMembers }, pointGains);
        }
    });
});

// Uptime window selector
uptimeWindowInputs.forEach(input => {
    input.addEventListener('change', (e) => {
        selectedUptimeWindow = parseInt(e.target.value);
        console.log(`Uptime window changed to ${selectedUptimeWindow}m`);
        
        if (currentClan && cachedMemberData && cachedHistoryData) {
            // Clear cache only for the previous window size
            // We keep other window sizes' cache intact
            const battleMembers = cachedMemberData.members.filter(m => m.battle_id === currentBattle);
            battleMembers.forEach(member => {
                if (!member.UserID) return;
                const oldCacheKey = `${member.UserID}_${e.target.defaultValue}`;
                delete uptimeCache[oldCacheKey];
            });
            
            // Calculate new uptimes for all members
            console.log('Recalculating uptimes for all members...');
            battleMembers.forEach(member => {
                if (!member.UserID) return;
                const uptime = calculateMemberUptime(cachedHistoryData, member.UserID, selectedUptimeWindow);
                lastUptimeValues.set(member.UserID, uptime);
                
                // Update the uptime cell directly
                const uptimeCell = document.querySelector(`.uptime-cell-${member.UserID}`);
                if (uptimeCell) {
                    uptimeCell.textContent = formatPercentage(uptime);
                }
                console.log(`Updated uptime for ${member.username}: ${uptime}%`);
            });
            
            // Calculate and update average uptime in stats
            const avgUptime = Array.from(lastUptimeValues.values()).reduce((sum, val) => sum + val, 0) / lastUptimeValues.size;
            avgUptimeElement.textContent = formatPercentage(avgUptime);
            console.log(`Updated average uptime: ${avgUptime}%`);
        }
    });
});

// Add CSS for loading spinner
const style = document.createElement('style');
style.textContent = `
.loading-spinner {
    display: inline-block;
    width: 16px;
    height: 16px;
    border: 2px solid #f3f3f3;
    border-top: 2px solid #3498db;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    vertical-align: middle;
}

@keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
}
`;
document.head.appendChild(style);

// Initialize the dashboard when loaded
document.addEventListener('DOMContentLoaded', async () => {
    // Populate battle selector before loading data
    await populateBattleSelector();

    // Set default clan or load saved clan
    const savedClan = loadSavedClan();
    clanSelect.value = savedClan;
    currentClan = savedClan;

    // Match default points period display
    pointsGainedPeriodElement.textContent = formatTimePeriod(60);

    handleClanChange();
    // Auto-refresh: use only the 24h history for lightweight updates
    setInterval(async () => {
        if (!currentClan || !cachedMemberData) return;
        try {
            const recentHistoryData = await fetchRecentHistory(currentClan);
            if (!recentHistoryData?.history?.length) return;
            cachedHistoryData = recentHistoryData;
            const battleMembers = cachedMemberData.members.filter(m => m.battle_id === currentBattle);
            const pointGains = calculatePointGains({ ...cachedMemberData, members: battleMembers }, recentHistoryData);
            renderMemberTable({ ...cachedMemberData, members: battleMembers }, pointGains);
            const stats = calculateStats({ ...cachedMemberData, members: battleMembers }, recentHistoryData, selectedUptimeWindow);
            updateStatsDisplay(stats);
            lastUpdatedElement.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
        } catch (error) {
            console.error('Error during recent-history auto-refresh:', error);
        }
    }, UPDATE_INTERVAL);
});

async function refreshData() {
    try {
        const memberData = await fetchMemberData(currentClan);
        if (!memberData?.battle_id) {
            console.error('No battle_id found in member data');
            return;
        }
        
        // Update battle select with latest battle
        updateBattleSelect(memberData);
        
        // Get history data
        const historyData = await fetchRecentHistory(currentClan);
        
        // Only show members who participated in this battle
        const battleMembers = memberData.members.filter(m => m.battle_id === memberData.battle_id);
        await updateMemberTable(battleMembers, historyData);
        
        // Schedule next refresh
        setTimeout(refreshData, REFRESH_INTERVAL);
    } catch (error) {
        console.error('Error refreshing data:', error);
        setTimeout(refreshData, REFRESH_INTERVAL);
    }
}

// --- Calculation Functions ---
function calculateInactiveTime(historyData, userId, uptimeWindow) {
    if (!historyData?.history?.length) {
        return 0;
    }

    const history = historyData.history;
    const blockSize = uptimeWindow / 2;
    let inactiveBlocks = 0;

    // Pre-extract points for this user
    const userPoints = [];
    for (const record of history) {
        const member = record.members.find(m => m.UserID === userId);
        if (member) {
            userPoints.push(member.points);
        }
    }

    // First check if inactive in current window
    let isCurrentlyInactive = true;
    for (let i = 0; i < blockSize; i++) {
        if (i + 1 >= userPoints.length) break;
        if (userPoints[i] !== userPoints[i + 1]) {
            isCurrentlyInactive = false;
            break;
        }
    }

    // If not currently inactive, return 0
    if (!isCurrentlyInactive) {
        return 0;
    }

    // Count inactive blocks until we find a point change
    for (let i = 0; i < userPoints.length - 1; i++) {
        if (userPoints[i] === userPoints[i + 1]) {
            inactiveBlocks++;
        } else {
            break;
        }
    }

    // Convert blocks to minutes (each block is 2 minutes)
    return inactiveBlocks * 2;
}