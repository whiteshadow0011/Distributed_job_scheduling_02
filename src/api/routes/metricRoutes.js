import { Router } from 'express';
import { getQueueMetrics, getGlobalOverviewMetrics, streamQueueMetrics } from '../contorller/metricController.js';
import { authenticateToken } from '../middleware/auth.js';

const router = Router({ mergeParams: true });

router.use(authenticateToken);
router.get('/overview', getGlobalOverviewMetrics);
router.get('/stream', streamQueueMetrics);
router.get('/', getQueueMetrics);

export default router;