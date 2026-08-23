import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { pool } from '../../config/db.js';

export async function register(req, res, next) {
  const { organizationName, email, password } = req.body;

  if (!organizationName || !email || !password) {
    return res.status(400).json({ error: 'organizationName, email, and password are required' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Create Organization
    const orgRes = await client.query(
      `INSERT INTO organizations (name) VALUES ($1) RETURNING id, name`,
      [organizationName]
    );
    const org = orgRes.rows[0];

    // 2. Hash Password & Create User
    const salt = await bcrypt.genSalt(10);
    const passwordHash = await bcrypt.hash(password, salt);

    const userRes = await client.query(
      `INSERT INTO users (organization_id, email, password_hash)
       VALUES ($1, $2, $3)
       RETURNING id, email, organization_id, created_at`,
      [org.id, email, passwordHash]
    );
    const user = userRes.rows[0];

    // 3. Create a Default Project for the Organization
    await client.query(
      `INSERT INTO projects (organization_id, name) VALUES ($1, $2)`,
      [org.id, 'Default Project']
    );

    await client.query('COMMIT');

    // 4. Generate JWT
    const token = jwt.sign(
      { id: user.id, organization_id: user.organization_id, email: user.email },
      process.env.JWT_SECRET || 'super_secret_jwt_key_12345',
      { expiresIn: '24h' }
    );

    res.status(201).json({
      message: 'Organization and user created successfully',
      token,
      user: { id: user.id, email: user.email, organization_id: user.organization_id },
    });
  } catch (error) {
    await client.query('ROLLBACK');
    if (error.code === '23505') { // Unique constraint violation (email)
      return res.status(409).json({ error: 'Email already in use' });
    }
    next(error);
  } finally {
    client.release();
  }
}

export async function login(req, res, next) {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: 'email and password are required' });
  }

  try {
    const userRes = await pool.query(
      `SELECT id, organization_id, email, password_hash FROM users WHERE email = $1`,
      [email]
    );

    if (userRes.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const user = userRes.rows[0];
    const isMatch = await bcrypt.compare(password, user.password_hash);

    if (!isMatch) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign(
      { id: user.id, organization_id: user.organization_id, email: user.email },
      process.env.JWT_SECRET || 'super_secret_jwt_key_12345',
      { expiresIn: '24h' }
    );

    res.json({
      message: 'Login successful',
      token,
      user: { id: user.id, email: user.email, organization_id: user.organization_id },
    });
  } catch (error) {
    next(error);
  }
}