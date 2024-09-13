module.exports = {
    apps: [{
        name: 'NFT-Terminal Asset Recorder',
        script: './dist/index.js',
        args: 'start',
        watch: false,
        instances: 1,
        exec_mode: 'fork',
        wait_ready: false,
        listen_timeout: 10000,
        kill_timeout: 5000,
        max_memory_restart: '4000M'
    }]
};
