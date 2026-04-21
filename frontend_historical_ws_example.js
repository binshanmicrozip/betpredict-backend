// Historical Data WebSocket Client Example
// Connect to ws://localhost:8000/ws/bet-signals/

class HistoricalDataClient {
    constructor() {
        this.ws = null;
        this.isConnected = false;
    }

    connect() {
        const wsUrl = 'ws://localhost:8000/ws/bet-signals/';

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = (event) => {
            console.log('Connected to historical data WebSocket');
            this.isConnected = true;
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
        };

        this.ws.onclose = (event) => {
            console.log('Disconnected from historical data WebSocket');
            this.isConnected = false;
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }

    handleMessage(data) {
        console.log('Received:', data.type, data);

        switch (data.type) {
            case 'connection_established':
                console.log('Connection established:', data.message);
                break;

            case 'cricket_history_response':
                this.displayCricketHistory(data);
                break;

            case 'market_history_response':
                this.displayMarketHistory(data);
                break;

            case 'combined_history_response':
                this.displayCombinedHistory(data);
                break;

            case 'error':
                console.error('Error:', data.message);
                break;

            default:
                console.log('Unknown message type:', data.type);
        }
    }

    // Request historical cricket data
    getCricketHistory(matchId, sourceMatchId, limit = 100) {
        if (!this.isConnected) {
            console.error('Not connected to WebSocket');
            return;
        }

        const request = {
            type: 'get_cricket_history',
            match_id: matchId,
            source_match_id: sourceMatchId,
            limit: limit
        };

        this.ws.send(JSON.stringify(request));
        console.log('Requested cricket history:', request);
    }

    // Request historical market data
    getMarketHistory(marketId, runnerId = null, limit = 100, hoursBack = 24) {
        if (!this.isConnected) {
            console.error('Not connected to WebSocket');
            return;
        }

        const request = {
            type: 'get_market_history',
            market_id: marketId,
            runner_id: runnerId,
            limit: limit,
            hours_back: hoursBack
        };

        this.ws.send(JSON.stringify(request));
        console.log('Requested market history:', request);
    }

    // Request combined cricket and market data
    getCombinedHistory(matchId, sourceMatchId, marketId, runnerId = null, limit = 50) {
        if (!this.isConnected) {
            console.error('Not connected to WebSocket');
            return;
        }

        const request = {
            type: 'get_combined_history',
            match_id: matchId,
            source_match_id: sourceMatchId,
            market_id: marketId,
            runner_id: runnerId,
            limit: limit
        };

        this.ws.send(JSON.stringify(request));
        console.log('Requested combined history:', request);
    }

    // Display methods (implement according to your UI)
    displayCricketHistory(data) {
        console.log(`Cricket History for Match ${data.match_id}:`);
        console.log(`${data.total_snapshots} snapshots, ${data.total_commentary} commentary items`);

        // Display cricket snapshots
        data.cricket_snapshots.forEach(snapshot => {
            console.log(`${snapshot.timestamp}: ${snapshot.score}/${snapshot.wickets} (${snapshot.overs} ov)`);
        });

        // Update your UI here
        // Example: updateCricketChart(data.cricket_snapshots);
    }

    displayMarketHistory(data) {
        console.log(`Market History for ${data.market_id}:`);
        console.log(`${data.total_ticks} ticks over last ${data.hours_back} hours`);

        // Display market ticks
        data.market_ticks.forEach(tick => {
            console.log(`${tick.tick_time}: LTP ${tick.ltp}`);
        });

        // Update your UI here
        // Example: updateMarketChart(data.market_ticks);
    }

    displayCombinedHistory(data) {
        console.log(`Combined History for Match ${data.match_id} / Market ${data.market_id}:`);
        console.log(`${data.total_cricket} cricket snapshots, ${data.total_market} market ticks`);

        // Display combined data
        const cricket = data.cricket_snapshots;
        const market = data.market_ticks;

        if (cricket.length > 0 && market.length > 0) {
            console.log('Latest combined data:');
            const lastCricket = cricket[cricket.length - 1];
            const lastMarket = market[market.length - 1];
            console.log(`Cricket: ${lastCricket.score}/${lastCricket.wickets} (${lastCricket.overs} ov)`);
            console.log(`Market: LTP ${lastMarket.ltp}`);
        }

        // Update your UI here
        // Example: updateCombinedChart(data.cricket_snapshots, data.market_ticks);
    }
}

// Usage example
const historicalClient = new HistoricalDataClient();

// Connect to WebSocket
historicalClient.connect();

// After connection is established, request data
setTimeout(() => {
    // Get cricket history
    historicalClient.getCricketHistory('151807', '151807', 20);

    // Get market history
    historicalClient.getMarketHistory('1.256856948', '67868736', 20, 2);

    // Get combined history
    historicalClient.getCombinedHistory('151807', '151807', '1.256856948', '67868736', 20);
}, 1000);

// Don't forget to disconnect when done
// historicalClient.disconnect();