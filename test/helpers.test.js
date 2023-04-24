const assert = require('assert');
const fs = require('fs-extra');
const helpers = require('../helpers')
const stream = require('node:stream/promises');

describe('Helpers', function() {
    describe('CSV pipes()', function() {
        it('should parse rows as objects', async function() {
            const parse = helpers.csvParser();

            const records = []
            parser = fs.createReadStream('./test/fixtures/sample.csv').pipe(parse);
            parser.on('readable', function() {
                    let record;
                    while ((record = this.read()) !== null) {
                        records.push(record);
                    }
                })
                .on('end', function() {});
            // wait streaming to finish
            await stream.finished(parser);

            assert.equal(
                records.length,
                100
            );
            // make sure the rows are parsed as objects
            assert.deepStrictEqual(
                records[0], {
                    "AdDeliveryMethod": "",
                    "AdDuration": "",
                    "AdUnitHash": "6f2b650341f3d810a40f94344244b6e6",
                    "AdUnitId": "33042948",
                    "AdUnitTypeId": "",
                    "AdsSelected": "",
                    "AiringId": "",
                    "AllEventKV": "_fw_session_id=4d5c6a7c-2333-4d23-a138-c2246fc9b2bb",
                    "AllRequestKV": "",
                    "AudienceItem": "",
                    "BillableAbstractEventId": "",
                    "BreakId": "",
                    "CBPId": "",
                    "ChannelId": "",
                    "ClockNumber": "",
                    "ConcreteEventId": "",
                    "ContentBrandId": "",
                    "ContentChannelId": "",
                    "ContentDuration": "3",
                    "ContentProviderPartnerId": "",
                    "CreativeDuration": "60",
                    "CreativeId": "31940250",
                    "CreativeRenditionId": "167778672",
                    "CustomVisitorId": "cfb45f36345f59f367b4f309df516741",
                    "DaypartId": "",
                    "DeviceId": "",
                    "DistributionPartnerId": "",
                    "EndpointId": "",
                    "EndpointOwnerId": "18",
                    "EventMultiplier": "",
                    "EventName": "defaultImpression",
                    "EventTime": "20220700000000",
                    "EventType": "I",
                    "ExternalReseller": "0",
                    "GIVTDecisionResult": "I-IRP",
                    "GenreId": "",
                    "GlobalAdvertiserId": "",
                    "GlobalBrandId": "",
                    "HyLDARequest": "",
                    "IPEnabledAudience": "2",
                    "Label": "quartiles_measurable",
                    "LanguageId": "",
                    "LinearDecisionType": "Linear Scheduled",
                    "ListingId": "",
                    "LogSampleAmplifier": "1",
                    "MRMRuleId": "",
                    "MarketAdSource": "",
                    "MarketAdvertiserId": "",
                    "MarketBuyerSeatId": "",
                    "MarketDealId": "",
                    "MarketUnifiedAdId": "",
                    "MatchedAudienceItem": "",
                    "MaxAds": "",
                    "MaxDuration": "",
                    "NetValue": "0",
                    "NetworkId": "384777",
                    "OpportunityId": "13670210784",
                    "PageViewRandom": "",
                    "ParsedUserAgent": "qamrdk;-;-;",
                    "PlacementId": "33042930",
                    "PlacementOpportunityId": "",
                    "PlatformBrowserId": "",
                    "PlatformDeviceId": "1350",
                    "PlatformGroup": "STB/Linear live",
                    "PlatformOSId": "",
                    "PositionInSlot": "0",
                    "PriceModel": "R",
                    "ProgOpenExchangeRuleId": "",
                    "ProgrammerId": "",
                    "PurchasedOrderId": "",
                    "RawUserAgent": "QAMRDK/PX013AN_5.0p21s1_PROD_sey",
                    "RecordIdentifier": "r7UFJ7Yq/1u2gKaRsSFq4Q==0",
                    "ReferrerURL": "",
                    "RequestAiringCustomId": "",
                    "RequestBreakCustomId": "",
                    "RequestChannelCustomId": "",
                    "RequestSiteCustomId": "",
                    "RequestVideoCustomId": "",
                    "SalesChannelType": "Programmatic",
                    "ScenarioId": "",
                    "SellingPartnerId": "",
                    "SeriesId": "",
                    "SignalId": "r8sI7txa3mIAAAAAAAABAQ==",
                    "SiteId": "765030",
                    "SiteSectionId": "9437036",
                    "SlotIndex": "0",
                    "SoldOrderID": "",
                    "StandardDeviceTypeId": "5",
                    "StandardEnvironmentId": "2",
                    "StandardOSId": "",
                    "StreamId": "5646490000000000000",
                    "StreamType": "1",
                    "TVRatingId": "6",
                    "TimePosition": "0",
                    "TimePositionClass": "midroll",
                    "TimeUnfilled": "",
                    "TransactionId": "1658739576003835365-o2fab",
                    "TriggeringConcreteEventId": "",
                    "UniqueIdentifier": "1ede087e7364302cbdb3be599ff44515",
                    "ValueChainRole": "R",
                    "VideoAssetId": "",
                    "VideoPlayRandom": "",
                    "VisitorCity": "",
                    "VisitorCountry": "us",
                    "VisitorStateProvince": "",
                    "VisitorTimeZoneOffset": "",
                    "VisitorZone": "5315",
                    "XifaId": "",
                });
        });

        it('should filter rows by criteria', async function() {
            const parse = helpers.csvParser();
            const rowsFilter = helpers.filterRows();

            const records = []
            parser = fs.createReadStream('./test/fixtures/sample.csv').pipe(parse).pipe(rowsFilter);
            parser.on('readable', function() {
                    let record;
                    while ((record = this.read()) !== null) {
                        records.push(record);
                    }
                })
                .on('end', function() {});
            // wait streaming to finish
            await stream.finished(parser);

            assert.equal(
                records.length,
                10
            );
            // make sure the rows are filtered
            for (let i = 0; i < records.length; i++) {
                const event = records[i];
                assert.equal(event.SalesChannelType === 'Programmatic', true);
                assert.equal(['defaultImpression', 'firstQuartile'].includes(event.EventName), true);
            }
        });

        it('should include only allowed columns', async function() {
            const parse = helpers.csvParser();
            const rowsFilter = helpers.filterRows();
            const columnsFilter = helpers.filterColumns();

            const records = []
            parser = fs.createReadStream('./test/fixtures/sample.csv').pipe(parse).pipe(rowsFilter).pipe(columnsFilter);
            parser.on('readable', function() {
                    let record;
                    while ((record = this.read()) !== null) {
                        records.push(record);
                    }
                })
                .on('end', function() {});
            // wait streaming to finish
            await stream.finished(parser);
            assert.equal(
                records.length,
                1
            );
            // make sure the columns are filtered
            const expectedResult = 'TransactionId,EventName,SiteId,SiteSectionId,PlatformGroup\n' +
                '1658739576003835365-o2fab,defaultImpression,765030,9437036,STB/Linear live\n' +
                '1658739544761248745-w8b69,firstQuartile,765030,9437036,STB/Linear live\n' +
                '1658739544761248745-w8b69,firstQuartile,765030,9437036,STB/Linear live\n' +
                '1658739544761248745-w8b69,defaultImpression,765030,9437036,STB/Linear live\n' +
                '1658739544761248745-w8b69,defaultImpression,765030,9437036,STB/Linear live\n' +
                '1658739544761248745-w8b69,defaultImpression,765030,9437036,STB/Linear live\n' +
                '1658739579205531369-o03e9,defaultImpression,765030,9437036,STB/Linear live\n' +
                '1658739579205531369-o03e9,firstQuartile,765030,9437036,STB/Linear live\n' +
                '1658739543468081450-a208,firstQuartile,765030,9437036,STB/Linear live\n' +
                '1658739543468081450-a208,firstQuartile,765030,9437036,STB/Linear live\n';

            assert.equal(expectedResult, records.join(''));
        });
    });
});