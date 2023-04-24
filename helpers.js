const axios = require('axios')
const csv = require('csv');
const path = require('path');

const getHttpStream = async (url, headers = {}) => {
    try {
        const res = await axios.get(url, {
            headers,
            responseType: "stream",
            decompress: false,
        });
        return res.data;
    } catch (err) {
        console.log('getHttpStream error: ', err);
        throw err;
    }
};

const getBasenameFromUrl = (urlStr) => {
    const url = new URL(urlStr);

    return path.basename(url.pathname);
}

// csv parser
const csvParser = () => csv.parse({
    columns: true
});

// csv rows filter
const filterRows = () => csv.transform(data => {
    // filter csv rows based on the required criteria
    if (data.SalesChannelType === 'Programmatic' && ['defaultImpression', 'firstQuartile'].includes(data.EventName)) {
        return data;
    }

    return null;
});

// csv columns picker
const filterColumns = () => csv.stringify({
    columns: ['TransactionId', 'EventName', 'SiteId', 'SiteSectionId', 'PlatformGroup'],
    header: true,
});

module.exports = {
    getHttpStream: getHttpStream,
    getBasenameFromUrl: getBasenameFromUrl,
    csvParser: csvParser,
    filterRows: filterRows,
    filterColumns: filterColumns,
};
