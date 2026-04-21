CREATE TABLE widgets (id INTEGER NOT NULL);
SELECT widgets.id FROM widgets, widgets AS other;
SELECT left_widget.id FROM widgets AS left_widget JOIN widgets AS right_widget USING (id);
SELECT public.widgets.id FROM widgets;
SELECT COUNT(*) FROM widgets;
