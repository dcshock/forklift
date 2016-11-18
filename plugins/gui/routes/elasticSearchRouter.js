var elasticController = require('../controllers/elasticSearchController.js');
var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;

module.exports = function(express) {
    var router = express.Router();
    router.post('/forklift-gui/fixed/', ensureAuthenticated, elasticController.updateAsFixed);
    router.post('/forklift-gui/fixAll/', ensureAuthenticated, elasticController.updateAllAsFixed);
    router.post('/forklift-gui/retry/', ensureAuthenticated, elasticController.retry);
    router.get('/forklift-gui/retries/', ensureAuthenticated, elasticController.showRetries);
    router.get('/forklift-gui/replays/', ensureAuthenticated, elasticController.showReplays);
    router.get('/forklift-gui/filtered/', ensureAuthenticated, elasticController.showFilteredResults);

    return router;
};
