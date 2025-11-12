import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  // TODO: Implement order creation with the following steps:
  // 1. Validate request body:
  //    - Check productId exists
  //    - Check quantity is positive
  // 2. Call product service to verify product exists:
  //    - Use axios to GET product details
  //    - Handle timeouts and errors
  // 3. Insert order into database:
  //    - Add to orders table with PENDING status
  // 4. Publish order.created event to message broker:
  //    - Include order id, product details, quantity
  // 5. Return success response with order details
  try {
    const { productId, quantity } = req.body;

    // Validate input
    if (!productId || !quantity || quantity <= 0) {
      return res.status(400).json({ error: 'productId and positive quantity required' });
    }

    // Verify product exists (via product service)
    let product;
    try {
      const r = await axios.get(`${process.env.PRODUCT_SERVICE_URL}/products/${productId}`, {
        timeout: 2000
      });
      product = r.data;
    } catch (err) {
      console.error('Product validation failed:', err.message);
      return res.status(400).json({ error: 'Invalid productId or product service unavailable' });
    }

    // Insert order into DB
    const result = await db.query(
      `INSERT INTO orders (product_id, quantity, status)
       VALUES ($1,$2,'PENDING') RETURNING *`,
      [productId, quantity]
    );
    const order = result.rows[0];

    // Publish event to RabbitMQ
    const event = {
      event: 'ORDER_CREATED',
      orderId: order.id,
      productId,
      quantity
    };

    await publishMessage('orders', event);
    console.log('✅ Published ORDER_CREATED event:', event);

    // Return response
    res.status(201).json({
      message: 'Order placed successfully',
      order,
    });

  } catch (err) {
    console.error('Order create error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
