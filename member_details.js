// Get URL parameters
const urlParams = new URLSearchParams(window.location.search);
const clan = urlParams.get('clan');
const userId = urlParams.get('userId');
const battle = urlParams.get('battle');

if (!clan || !userId || !battle) {
    document.body.innerHTML = '<div class="container mt-5"><h1>Error: Missing required parameters</h1><p>Please ensure clan, userId, and battle are provided in the URL.</p></div>';
    throw new Error('Missing required parameters');
}

// Constants
const API_BASE_URL = 'http://127.0.0.1:8001';
const LOADING_PLACEHOLDER = '<span class="loading-spinner"></span>';

// --- DOM Elements ---
const memberNameElement = document.getElementById('member-name');
const memberClanElement = document.getElementById('member-clan');
const currentPointsElement = document.getElementById('current-points');
const currentRankElement = document.getElementById('current-rank');
const avgUptimeElement = document.getElementById('avg-uptime');
const pointsPerHourElement = document.getElementById('points-per-hour');
const memberBattleElement = document.getElementById('member-battle');
const lastUpdatedElement = document.getElementById('last-updated');
const memberStatusElement = document.getElementById('member-status');

// --- Charts ---
let pointsGainedChart = null;
let activityTimelineChart = null;
let rankHistoryChart = null;
let pointsHistoryChart = null;

// --- Global Variables ---
let memberData = null;
let historyData = null;  // Global cache for history data
let memberHistory = null;  // Member-specific filtered history
let fullHistoryData = null;  // Cache for full history data

// --- Utility Functions ---
function formatNumber(num) {
    return num.toLocaleString();
}

function formatPercentage(num) {
    return (num * 100).toFixed(1) + '%';
}

function formatRelativeTime(timestamp) {
    const now = new Date();
    const diffInMinutes = Math.floor((now - timestamp) / (1000 * 60));
    
    if (diffInMinutes < 1) return 'Last updated just now';
    if (diffInMinutes < 60) return `${diffInMinutes} minute${diffInMinutes === 1 ? '' : 's'} ago`;
    
    const diffInHours = Math.floor(diffInMinutes / 60);
    if (diffInHours < 24) return `Last updated ${diffInHours} hour${diffInHours === 1 ? '' : 's'} ago`;
    
    const diffInDays = Math.floor(diffInHours / 24);
    return `${diffInDays} day${diffInDays === 1 ? '' : 's'} ago`;
}

// --- Data Processing Functions ---
function calculatePointGains(historyData, minutes = 60) {
    if (!historyData?.length) return [];

    const gains = [];
    const sortedHistory = [...historyData].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
    );

    // Find the most recent timestamp and work backwards
    const mostRecentTimestamp = new Date(sortedHistory[sortedHistory.length - 1].timestamp);
    const targetTime = new Date(mostRecentTimestamp - minutes * 60000);

    // Filter and sort relevant history
    const relevantHistory = sortedHistory
        .filter(record => new Date(record.timestamp) >= targetTime);

    // Calculate point gains between consecutive records
    for (let i = 1; i < relevantHistory.length; i++) {
        const prevPoints = relevantHistory[i - 1].points;
        const currentPoints = relevantHistory[i].points;
        gains.push({
            timestamp: new Date(relevantHistory[i].timestamp),
            gain: currentPoints - prevPoints
        });
    }

    return gains;
}

function calculateHourlyComparison(historyData) {
    // Sort history by timestamp in ascending order
    const sortedHistory = [...historyData].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
    );

    if (sortedHistory.length === 0) return { currentHour: [], previousHour: [] };

    // Find the most recent timestamp in the data
    const mostRecentTimestamp = new Date(sortedHistory[sortedHistory.length - 1].timestamp);
    const oneHourAgo = new Date(mostRecentTimestamp - 60 * 60000);
    const twoHoursAgo = new Date(mostRecentTimestamp - 120 * 60000);

    // Split history into current and previous hour
    const currentHourData = [];
    const previousHourData = [];

    let lastPoints = null;
    let lastTimestamp = null;

    sortedHistory.forEach(record => {
        const timestamp = new Date(record.timestamp);
        const points = record.points;

        if (lastPoints !== null && lastTimestamp !== null) {
            const pointGain = points - lastPoints;
            const gainRecord = {
                timestamp: timestamp,
                gain: pointGain
            };

            if (timestamp >= oneHourAgo && timestamp <= mostRecentTimestamp) {
                currentHourData.push(gainRecord);
            } else if (timestamp >= twoHoursAgo && timestamp < oneHourAgo) {
                // Shift previous hour data by 1 hour to overlay with current hour
                const shiftedTimestamp = new Date(timestamp.getTime() + 60 * 60000);
                previousHourData.push({
                    timestamp: shiftedTimestamp,
                    gain: pointGain
                });
            }
        }

        lastPoints = points;
        lastTimestamp = timestamp;
    });

    console.log('Data time ranges:', {
        mostRecent: mostRecentTimestamp.toISOString(),
        oneHourAgo: oneHourAgo.toISOString(),
        twoHoursAgo: twoHoursAgo.toISOString(),
        currentHourPoints: currentHourData.length,
        previousHourPoints: previousHourData.length
    });

    return {
        currentHour: currentHourData,
        previousHour: previousHourData
    };
}

function calculateRankHistory(historyData, memberId) {
    return historyData.map(record => {
        const members = record.members.sort((a, b) => b.points - a.points);
        const rank = members.findIndex(m => m.UserID === memberId) + 1;
        return {
            timestamp: new Date(record.timestamp),
            rank
        };
    });
}

// --- Chart Creation Functions ---
function createPointsGainedChart(hourlyData) {
    const ctx = document.getElementById('points-gained-chart').getContext('2d');
    
    const currentHourData = hourlyData.currentHour;
    const previousHourData = hourlyData.previousHour;

    // Prepare datasets
    const datasets = [
        {
            label: 'Current Hour',
            data: currentHourData.map(d => ({ x: d.timestamp, y: d.gain })),
            borderColor: '#007bff',
            fill: false
        },
        {
            label: 'Previous Hour',
            data: previousHourData.map(d => ({ x: d.timestamp, y: d.gain })),
            borderColor: '#6c757d',
            fill: false,
            borderDash: [5, 5]
        }
    ];

    return new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute',
                        displayFormats: {
                            minute: 'HH:mm'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Points Gained'
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        title: (context) => {
                            return new Date(context[0].parsed.x).toLocaleTimeString();
                        }
                    }
                }
            }
        }
    });
}

function createActivityTimeline(memberHistory) {
    const ctx = document.getElementById('activity-timeline').getContext('2d');
    
    if (!memberHistory?.length) {
        return new Chart(ctx, {
            type: 'bar',
            data: { datasets: [{ data: [] }] },
            options: { responsive: true }
        });
    }

    // Sort history by timestamp in ascending order
    const sortedHistory = [...memberHistory].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
    );

    const twoMinutes = 2 * 60 * 1000; // 2 minutes in milliseconds
    const timelineData = [];
    let activeBlockCount = 0;
    let inactiveBlockCount = 0;

    // Find start and end times for the entire range
    const startTime = new Date(sortedHistory[0].timestamp);
    const endTime = new Date(sortedHistory[sortedHistory.length - 1].timestamp);
    
    // Create a map of inactive periods
    const inactiveBlockMap = new Map(); // timestamp -> boolean
    for (let i = 1; i < sortedHistory.length; i++) {
        const prevTime = new Date(sortedHistory[i - 1].timestamp);
        const currentTime = new Date(sortedHistory[i].timestamp);
        const prevPoints = sortedHistory[i - 1].points;
        const currentPoints = sortedHistory[i].points;
        
        if (currentPoints === prevPoints) {
            // Mark all blocks between prevTime and currentTime as inactive
            let blockTime = new Date(Math.floor(prevTime.getTime() / twoMinutes) * twoMinutes);
            const endBlockTime = new Date(Math.ceil(currentTime.getTime() / twoMinutes) * twoMinutes);
            
            while (blockTime < endBlockTime) {
                inactiveBlockMap.set(blockTime.getTime(), true);
                blockTime = new Date(blockTime.getTime() + twoMinutes);
            }
        }
    }

    // Generate blocks for the entire time range
    let currentBlock = new Date(Math.floor(startTime.getTime() / twoMinutes) * twoMinutes);
    const lastBlock = new Date(Math.ceil(endTime.getTime() / twoMinutes) * twoMinutes);

    while (currentBlock <= lastBlock) {
        const blockTime = currentBlock.getTime();
        const isInactive = inactiveBlockMap.has(blockTime);
        
        timelineData.push({
            x: new Date(blockTime),
            y: 1
        });

        if (isInactive) {
            inactiveBlockCount++;
        } else {
            activeBlockCount++;
        }

        currentBlock = new Date(blockTime + twoMinutes);
    }

    // Create chart
    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            datasets: [{
                data: timelineData,
                backgroundColor: (context) => {
                    const blockTime = context.raw?.x?.getTime();
                    return inactiveBlockMap.has(blockTime) ? '#dc3545' : '#28a745';
                },
                borderWidth: 0,
                barPercentage: 1.0,
                categoryPercentage: 1.0,
                borderSkipped: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'hour',
                        displayFormats: {
                            hour: 'HH:mm'
                        }
                    },
                    grid: {
                        display: false
                    },
                    ticks: {
                        display: false
                    },
                    offset: false
                },
                y: {
                    display: false,
                    grid: {
                        display: false
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        title: (context) => {
                            return new Date(context[0].raw.x).toLocaleTimeString();
                        },
                        label: (context) => {
                            const blockTime = context.raw.x.getTime();
                            return inactiveBlockMap.has(blockTime) ? 'Inactive' : 'Active';
                        }
                    }
                }
            }
        }
    });

    // Add block counts below the chart
    const totalBlocks = activeBlockCount + inactiveBlockCount;
    const activePercentage = (activeBlockCount / totalBlocks * 100).toFixed(1);
    const inactivePercentage = (inactiveBlockCount / totalBlocks * 100).toFixed(1);
    
    // Remove any existing stats div
    const existingStats = ctx.canvas.parentNode.querySelector('.activity-stats');
    if (existingStats) {
        existingStats.remove();
    }
    
    const statsDiv = document.createElement('div');
    statsDiv.className = 'activity-stats';
    statsDiv.style.textAlign = 'center';
    statsDiv.style.marginTop = '10px';
    statsDiv.innerHTML = `Active: ${activeBlockCount} (${activePercentage}%) | Inactive: ${inactiveBlockCount} (${inactivePercentage}%)`;
    ctx.canvas.parentNode.appendChild(statsDiv);

    return chart;
}

function createRankHistoryChart(historyData) {
    const ctx = document.getElementById('rank-history-chart').getContext('2d');
    
    if (!historyData?.length) {
        return new Chart(ctx, {
            type: 'line',
            data: { datasets: [{ data: [] }] },
            options: { 
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        top: 10,
                        bottom: 20
                    }
                }
            }
        });
    }

    const data = historyData.map(record => {
        const members = record.members.sort((a, b) => b.points - a.points);
        const rank = members.findIndex(m => m.UserID === userId) + 1;
        return {
            x: new Date(record.timestamp),
            y: rank
        };
    });

    return new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Rank',
                data: data,
                borderColor: '#007bff',
                fill: false,
                stepped: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: {
                padding: {
                    top: 10,
                    right: 10,
                    bottom: 20,
                    left: 10
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'hour',
                        displayFormats: {
                            hour: 'EEE, HH:mm'
                        }
                    },
                    title: {
                        display: false
                    }
                },
                y: {
                    reverse: true,
                    title: {
                        display: false
                    },
                    ticks: {
                        precision: 0
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        title: (context) => {
                            const date = new Date(context[0].parsed.x);
                            return date.toLocaleDateString(undefined, { weekday: 'short' }) + ', ' + 
                                   date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false });
                        },
                        label: (context) => {
                            return `Rank: #${context.parsed.y}`;
                        }
                    }
                }
            }
        }
    });
}

function createPointsHistoryChart(memberHistory) {
    const ctx = document.getElementById('points-history-chart').getContext('2d');
    
    if (!memberHistory?.length) {
        return new Chart(ctx, {
            type: 'line',
            data: { datasets: [{ data: [] }] },
            options: { responsive: true }
        });
    }

    // Use the filtered member history directly
    const pointsData = memberHistory.map(record => ({
        x: new Date(record.timestamp),
        y: record.points
    }));

    // Sort data by timestamp
    pointsData.sort((a, b) => a.x - b.x);

    // Store the original time range
    const minTime = pointsData[0].x;
    const maxTime = pointsData[pointsData.length - 1].x;
    const timeRange = maxTime - minTime;

    // Create chart
    const chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Points',
                data: pointsData,
                borderColor: '#007bff',
                fill: false,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                zoom: {
                    pan: {
                        enabled: true,
                        mode: 'x',
                        modifierKey: 'ctrl'
                    },
                    zoom: {
                        wheel: {
                            enabled: true,
                            modifierKey: 'ctrl'
                        },
                        pinch: {
                            enabled: true
                        },
                        mode: 'x'
                    },
                    limits: {
                        x: {min: minTime, max: maxTime}
                    }
                },
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        title: (context) => {
                            return new Date(context[0].parsed.x).toLocaleString();
                        },
                        label: (context) => {
                            return `Points: ${context.parsed.y.toLocaleString()}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'hour',
                        displayFormats: {
                            hour: 'MMM d, HH:mm'
                        }
                    },
                    title: {
                        display: false  // Removed x-axis label
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: false  // Removed y-axis label
                    }
                }
            }
        }
    });

    // Initialize the range slider
    const slider = document.getElementById('points-range-slider');
    const timeStart = document.getElementById('time-start');
    const timeEnd = document.getElementById('time-end');

    // Add toggle switch for points/gains view
    const toggleContainer = document.createElement('div');
    toggleContainer.style.display = 'inline-block';
    toggleContainer.style.marginRight = '10px';
    
    const toggleSwitch = document.createElement('label');
    toggleSwitch.className = 'switch';
    toggleSwitch.innerHTML = `
        <span class="toggle-label left">Points</span>
        <input type="checkbox" id="view-mode-toggle">
        <span class="slider round"></span>
        <span class="toggle-label right">Gains</span>
    `;
    
    const resetButton = document.getElementById('reset-zoom-btn');
    resetButton.parentNode.insertBefore(toggleContainer, resetButton);
    toggleContainer.appendChild(toggleSwitch);

    // Add toggle switch styles
    const style = document.createElement('style');
    style.textContent = `
        .switch {
            position: relative;
            display: inline-flex;
            align-items: center;
            vertical-align: middle;
            margin-right: 5px;
        }
        .switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }
        .switch .slider {
            position: relative;
            display: inline-block;
            width: 40px;
            height: 20px;
            background-color: #ccc;
            border-radius: 20px;
            transition: .4s;
            vertical-align: middle;
            cursor: pointer;
            margin: 0 5px;
        }
        .switch .slider:before {
            position: absolute;
            content: "";
            height: 16px;
            width: 16px;
            left: 2px;
            bottom: 2px;
            background-color: white;
            border-radius: 50%;
            transition: .4s;
        }
        .switch input:checked + .slider {
            background-color: #2196F3;
        }
        .switch input:checked + .slider:before {
            transform: translateX(20px);
        }
        .toggle-label {
            font-size: 14px;
            vertical-align: middle;
        }
        .toggle-label.left {
            margin-right: 5px;
        }
        .toggle-label.right {
            margin-left: 5px;
        }
    `;
    document.head.appendChild(style);

    // Initialize gains cache
    const gainsCache = {
        lastCalculatedTimestamp: null,
        periods: []
    };

    // Function to calculate 4-minute period gains
    function calculatePeriodGains(data) {
        const gains = [];
        for (let i = 0; i < data.length - 1; i += 2) {
            if (i + 1 < data.length) {
                const startPoint = data[i];
                const endPoint = data[i + 1];
                gains.push({
                    x: endPoint.x,
                    y: endPoint.y - startPoint.y
                });
            }
        }
        return gains;
    }

    // Handle view mode toggle
    const viewModeToggle = document.getElementById('view-mode-toggle');
    viewModeToggle.addEventListener('change', (e) => {
        const showGains = e.target.checked;
        
        if (showGains && gainsCache.periods.length === 0) {
            // Calculate gains for the first time
            gainsCache.periods = calculatePeriodGains(pointsData);
            gainsCache.lastCalculatedTimestamp = pointsData[pointsData.length - 1].x;
        }

        // Update chart data
        chart.data.datasets[0].data = showGains ? gainsCache.periods : pointsData;
        chart.data.datasets[0].label = showGains ? 'Points Gained' : 'Points';

        // Update tooltip
        chart.options.plugins.tooltip.callbacks.label = (context) => {
            const value = context.parsed.y;
            return showGains ? 
                `Gained: ${value.toLocaleString()}` :
                `Points: ${value.toLocaleString()}`;
        };

        // Trigger slider update to adjust Y axis
        const [start, end] = slider.noUiSlider.get();
        slider.noUiSlider.set([start, end]);
    });

    noUiSlider.create(slider, {
        start: [minTime.getTime(), maxTime.getTime()],
        connect: true,
        range: {
            'min': minTime.getTime(),
            'max': maxTime.getTime()
        },
        step: 60000, // 1 minute steps
        margin: 60000, // Minimum 1 minute between handles
        behaviour: 'drag-tap' // Allow dragging and tapping
    });

    // Handle slider events
    slider.noUiSlider.on('update', (values, handle) => {
        const [start, end] = values.map(v => new Date(parseInt(v)));
        timeStart.textContent = start.toLocaleString();
        timeEnd.textContent = end.toLocaleString();

        // Get visible data points and adjust Y axis
        const visibleData = chart.data.datasets[0].data.filter(d => d.x >= start && d.x <= end);
        if (visibleData.length > 0) {
            const minPoints = Math.min(...visibleData.map(d => d.y));
            const maxPoints = Math.max(...visibleData.map(d => d.y));
            const range = maxPoints - minPoints;
            const padding = range * 0.05; // 5% padding

            // Calculate appropriate step size based on data range
            const maxWithPadding = maxPoints + padding;
            let step;
            if (maxWithPadding <= 100) step = 10;
            else if (maxWithPadding <= 1000) step = 100;
            else if (maxWithPadding <= 10000) step = 1000;
            else if (maxWithPadding <= 100000) step = 10000;
            else step = 200000;

            // Round to nice numbers for the axis
            const maxNice = Math.ceil((maxPoints + padding) / step) * step;
            const minNice = Math.floor(Math.max(0, minPoints - padding) / step) * step;

            // Update chart axes
            chart.options.scales.x.min = start;
            chart.options.scales.x.max = end;
            chart.options.scales.y.min = minNice;
            chart.options.scales.y.max = maxNice;
            chart.options.scales.y.ticks = {
                stepSize: step
            };
            chart.update('none');
        }
    });

    // Reset zoom button functionality
    document.getElementById('reset-zoom-btn').addEventListener('click', () => {
        slider.noUiSlider.set([minTime.getTime(), maxTime.getTime()]);
        chart.resetZoom();
    });

    return chart;
}

// --- Data Fetching Functions ---
async function fetchMemberData() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/member-tracking/${clan}`);
        if (!response.ok) throw new Error('Failed to fetch member data');
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching member data:', error);
        return null;
    }
}

async function fetchMemberHistory(userId = null) {
    try {
        const params = new URLSearchParams();
        if (userId) {
            console.log('Adding userId to params:', userId);
            params.append('userId', userId);
        }
        if (battle) {
            console.log('Adding battle_id to params:', battle);
            params.append('battle_id', battle);
        }
        
        const queryString = params.toString();
        const url = `${API_BASE_URL}/api/member-history/${clan}${queryString ? '?' + queryString : ''}`;
        console.log('Fetching history with URL:', url);
        
        const response = await fetch(url);
        if (!response.ok) throw new Error('Failed to fetch member history');
        const data = await response.json();
        console.log('Received history data:', {
            totalRecords: data.history.length,
            sampleRecord: data.history[0],
            membersInFirstRecord: data.history[0].members.length
        });
        return data;
    } catch (error) {
        console.error('Error fetching member history:', error);
        return null;
    }
}

// Add function for full history (for rank chart)
async function fetchFullHistory() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/member-history/${clan}`);
        if (!response.ok) throw new Error('Failed to fetch full history');
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching full history:', error);
        return null;
    }
}

// --- Initialization ---
async function initialize() {
    try {
        // Get member data first
        memberData = await fetchMemberData();
        if (!memberData) {
            throw new Error('Failed to fetch member data');
        }

        // Find the current member
        const member = memberData.members.find(m => m.UserID === userId);
        if (!member) {
            throw new Error('Member not found');
        }

        // Update member info immediately
        memberNameElement.textContent = member.username;
        memberClanElement.textContent = clan;
        memberBattleElement.textContent = battle;
        currentPointsElement.textContent = formatNumber(member.points);

        // Get member-specific history (single request with userId filter)
        historyData = await fetchMemberHistory(userId);
        if (!historyData) {
            throw new Error('Failed to fetch member history');
        }

        // Process member history once
        memberHistory = historyData.history.map(record => {
            const memberData = record.members.find(m => m.UserID === userId);
            return {
                timestamp: record.timestamp,
                points: memberData.points,
                members: record.members
            };
        });

        // Calculate all stats once
        const currentRank = memberData.members
            .sort((a, b) => b.points - a.points)
            .findIndex(m => m.UserID === userId) + 1;
        const inactiveTime = calculateInactiveTime(historyData, userId);
        const uptime = calculateUptime(memberHistory);
        const pointGains = calculatePointGains(memberHistory, 60);
        const totalGains = pointGains.reduce((sum, gain) => sum + gain.gain, 0);
        const hourlyComparison = calculateHourlyComparison(memberHistory);

        // Update all stats
        currentRankElement.textContent = formatNumber(currentRank);
        updateMemberStatus(inactiveTime);
        avgUptimeElement.textContent = formatPercentage(uptime);
        pointsPerHourElement.textContent = formatNumber(totalGains);

        // Clean up existing charts
        if (pointsGainedChart) pointsGainedChart.destroy();
        if (activityTimelineChart) activityTimelineChart.destroy();
        if (rankHistoryChart) rankHistoryChart.destroy();
        if (pointsHistoryChart) pointsHistoryChart.destroy();

        // Create all charts using the cached data
        pointsGainedChart = createPointsGainedChart(hourlyComparison);
        activityTimelineChart = createActivityTimeline(memberHistory);
        pointsHistoryChart = createPointsHistoryChart(memberHistory);

        // Add rank history load button
        const rankChartContainer = document.getElementById('rank-history-chart').parentElement;
        const loadRankButton = document.createElement('button');
        loadRankButton.className = 'btn btn-primary mb-3';
        loadRankButton.style.marginBottom = '15px';  // Add space below button
        loadRankButton.style.display = 'block';  // Make button full width
        loadRankButton.textContent = 'Load Rank History';
        loadRankButton.onclick = loadRankHistory;
        rankChartContainer.insertBefore(loadRankButton, rankChartContainer.firstChild);

        // Update chart container style for better padding
        const chartCanvas = document.getElementById('rank-history-chart');
        chartCanvas.style.marginTop = '10px';  // Add space between button and chart
        chartCanvas.style.marginBottom = '20px';  // Add space at bottom for axis labels

        // Update last updated time
        const lastUpdated = new Date(historyData.history[0].timestamp);
        lastUpdatedElement.textContent = formatRelativeTime(lastUpdated);

        // Set up auto-refresh with optimized data fetching
        setInterval(async () => {
            try {
                // Fetch both sets of data in parallel
                const [newMemberData, newHistoryData] = await Promise.all([
                    fetchMemberData(),
                    fetchMemberHistory(userId)  // Use userId filter here too
                ]);

                if (newMemberData && newHistoryData) {
                    // Update cached data
                    memberData = newMemberData;
                    historyData = newHistoryData;
                    memberHistory = newHistoryData.history.map(record => ({
                        timestamp: record.timestamp,
                        points: record.members[0].points,
                        members: record.members
                    }));

                    // Update member info
                    const updatedMember = memberData.members.find(m => m.UserID === userId);
                    if (updatedMember) {
                        currentPointsElement.textContent = formatNumber(updatedMember.points);
                        
                        // Update rank
                        const newRank = memberData.members
                            .sort((a, b) => b.points - a.points)
                            .findIndex(m => m.UserID === userId) + 1;
                        currentRankElement.textContent = formatNumber(newRank);
                    }

                    // Update status and time
                    const newInactiveTime = calculateInactiveTime(historyData, userId);
                    updateMemberStatus(newInactiveTime);
                    const newLastUpdated = new Date(historyData.history[0].timestamp);
                    lastUpdatedElement.textContent = formatRelativeTime(newLastUpdated);

                    // Update charts with new data
                    const newHourlyComparison = calculateHourlyComparison(memberHistory);
                    pointsGainedChart.data.datasets[0].data = newHourlyComparison.currentHour.map(d => ({ x: d.timestamp, y: d.gain }));
                    pointsGainedChart.data.datasets[1].data = newHourlyComparison.previousHour.map(d => ({ x: d.timestamp, y: d.gain }));
                    pointsGainedChart.update();

                    activityTimelineChart = createActivityTimeline(memberHistory);

                    pointsHistoryChart.data.datasets[0].data = memberHistory.map(record => ({
                        x: new Date(record.timestamp),
                        y: record.points
                    })).sort((a, b) => a.x - b.x);
                    pointsHistoryChart.update();
                }
            } catch (error) {
                console.error('Error during auto-refresh:', error);
            }
        }, 120000); // Refresh every 2 minutes

    } catch (error) {
        console.error('Error during initialization:', error);
        showError('Failed to initialize page. Please try refreshing.');
    }
}

// Add uptime calculation function
function calculateUptime(memberHistory) {
    if (!memberHistory?.length) return 0;

    const sortedHistory = [...memberHistory].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
    );

    let activeCount = 0;
    let totalCount = 0;

    for (let i = 0; i < sortedHistory.length - 1; i++) {
        const current = sortedHistory[i].points;
        const next = sortedHistory[i + 1].points;
        if (current !== next) {
            activeCount++;
        }
        totalCount++;
    }

    const result = totalCount > 0 ? (activeCount / totalCount) : 0;
    return result;
}

// Add this function to update the status
function updateMemberStatus(inactiveTime) {
    if (!inactiveTime || inactiveTime === 0) {
        memberStatusElement.textContent = 'ACTIVE';
        memberStatusElement.classList.add('active');
        memberStatusElement.classList.remove('inactive');
    } else {
        // Format the inactive time into hours and minutes
        const hours = Math.floor(inactiveTime / 60);
        const minutes = inactiveTime % 60;
        const timeStr = hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
        
        memberStatusElement.textContent = `⚠ INACTIVE - ${timeStr} ⚠`;
        memberStatusElement.classList.add('inactive');
        memberStatusElement.classList.remove('active');
    }
}

// Add the inactive time calculation function from dashboard
function calculateInactiveTime(historyData, userId) {
    if (!historyData?.history?.length) {
        return 0;
    }

    const history = historyData.history;
    const blockSize = 1; // Fixed 2-minute window (1 block = 2 minutes)
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

// Add rank history loading function
async function loadRankHistory() {
    try {
        const button = document.querySelector('#rank-history-chart').parentElement.querySelector('button');
        const originalText = button.textContent;
        button.disabled = true;
        button.textContent = 'Loading...';

        // Fetch full history if not cached
        if (!fullHistoryData) {
            fullHistoryData = await fetchFullHistory();
            if (!fullHistoryData) {
                throw new Error('Failed to fetch full history');
            }
        }

        // Create or update rank history chart
        if (rankHistoryChart) rankHistoryChart.destroy();
        rankHistoryChart = createRankHistoryChart(fullHistoryData.history);

        // Change button to refresh button
        button.textContent = 'Refresh Rank History';
        button.disabled = false;
    } catch (error) {
        console.error('Error loading rank history:', error);
        const button = document.querySelector('#rank-history-chart').parentElement.querySelector('button');
        button.textContent = 'Retry Loading Rank History';
        button.disabled = false;
    }
}

// Initialize when the page loads
document.addEventListener('DOMContentLoaded', initialize); 