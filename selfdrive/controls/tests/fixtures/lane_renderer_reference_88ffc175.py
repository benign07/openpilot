"""Frozen three production methods from88ffc175 renderer, full source SHA256 651053d6c72788d554c88c9f7b913a524fbacf619f001cc70491d8ecd684b3fa. Drawing oracle only."""
class ModelRenderer:

    @staticmethod
    def _get_path_length_idx(pos_x_array: np.ndarray, path_distance: float) -> int:
        """Get the index corresponding to the given path distance"""
        if len(pos_x_array) == 0:
            return 0
        indices = np.where(pos_x_array <= path_distance)[0]
        return indices[-1] if indices.size > 0 else 0

    def _map_line_to_polygon(self, line: np.ndarray, y_off: float, z_off: float, max_idx: int, max_distance: float, allow_invert: bool=True, y_shift: float=0.0, start_idx: int=0) -> np.ndarray:
        """Convert 3D line to 2D polygon for rendering."""
        if line.shape[0] == 0:
            return np.empty((0, 2), dtype=np.float32)
        points = line[start_idx:max_idx + 1]
        if 0 < max_idx < line.shape[0] - 1:
            p0 = line[max_idx]
            p1 = line[max_idx + 1]
            x0, x1 = (p0[0], p1[0])
            interp_y = np.interp(max_distance, [x0, x1], [p0[1], p1[1]])
            interp_z = np.interp(max_distance, [x0, x1], [p0[2], p1[2]])
            interp_point = np.array([max_distance, interp_y, interp_z], dtype=points.dtype)
            points = np.concatenate((points, interp_point[None, :]), axis=0)
        points = points[points[:, 0] >= 0]
        if points.shape[0] == 0:
            return np.empty((0, 2), dtype=np.float32)
        N = points.shape[0]
        offsets = np.array([[0, -y_off + y_shift, z_off], [0, y_off + y_shift, z_off]], dtype=np.float32)
        points_3d = points[None, :, :] + offsets[:, None, :]
        points_3d = points_3d.reshape(2 * N, 3)
        proj = self._car_space_transform @ points_3d.T
        proj = proj.reshape(3, 2, N)
        left_proj = proj[:, 0, :]
        right_proj = proj[:, 1, :]
        valid_proj = (np.abs(left_proj[2]) >= 1e-06) & (np.abs(right_proj[2]) >= 1e-06)
        if not np.any(valid_proj):
            return np.empty((0, 2), dtype=np.float32)
        left_screen = left_proj[:2, valid_proj] / left_proj[2, valid_proj][None, :]
        right_screen = right_proj[:2, valid_proj] / right_proj[2, valid_proj][None, :]
        clip = self._clip_region
        x_min, x_max = (clip.x, clip.x + clip.width)
        y_min, y_max = (clip.y, clip.y + clip.height)
        left_in_clip = (left_screen[0] >= x_min) & (left_screen[0] <= x_max) & (left_screen[1] >= y_min) & (left_screen[1] <= y_max)
        right_in_clip = (right_screen[0] >= x_min) & (right_screen[0] <= x_max) & (right_screen[1] >= y_min) & (right_screen[1] <= y_max)
        both_in_clip = left_in_clip & right_in_clip
        if not np.any(both_in_clip):
            return np.empty((0, 2), dtype=np.float32)
        left_screen = left_screen[:, both_in_clip]
        right_screen = right_screen[:, both_in_clip]
        if not allow_invert and left_screen.shape[1] > 1:
            y = left_screen[1, :]
            keep = y == np.minimum.accumulate(y)
            if not np.any(keep):
                return np.empty((0, 2), dtype=np.float32)
            left_screen = left_screen[:, keep]
            right_screen = right_screen[:, keep]
        return np.vstack((left_screen.T, right_screen[:, ::-1].T)).astype(np.float32)

    def _draw_lane_lines_carrot(self, sm):
        if self._carrot_show_lane_info < 1:
            return
        if not sm.valid['modelV2'] or not sm.valid['carState']:
            return
        model = sm['modelV2']
        car_state = sm['carState']
        lane_lines_raw = [np.array([ll.x, ll.y, ll.z], dtype=np.float32).T for ll in model.laneLines]
        road_edges_raw = [np.array([re.x, re.y, re.z], dtype=np.float32).T for re in model.roadEdges]
        lane_line_probs = np.array(model.laneLineProbs, dtype=np.float32)
        road_edge_stds = np.array(model.roadEdgeStds, dtype=np.float32)
        if lane_lines_raw[0].shape[0] == 0:
            return
        max_idx = self._get_path_length_idx(lane_lines_raw[0][:, 0], np.clip(lane_lines_raw[0][-1, 0], MIN_DRAW_DISTANCE, MAX_DRAW_DISTANCE))
        left_lane_line = car_state.leftLaneLine
        right_lane_line = car_state.rightLaneLine
        left_lane_color = left_lane_line // 10
        right_lane_color = right_lane_line // 10
        lane_colors = {2: (218, 202, 37), 3: (50, 150, 255)}
        lane_vertices = []
        lane_vertices_double = np.empty((0, 2), dtype=np.float32)
        for i in range(4):
            line_width = 0.025
            if i == 1 and left_lane_color in lane_colors:
                line_width = 0.05
            pts = self._map_line_to_polygon(lane_lines_raw[i], line_width, 0.0, max_idx, np.clip(lane_lines_raw[0][-1, 0], MIN_DRAW_DISTANCE, MAX_DRAW_DISTANCE))
            lane_vertices.append(pts)
            if i == 1:
                lane_vertices_double = self._map_line_to_polygon(lane_lines_raw[i], line_width, 0.0, max_idx, np.clip(lane_lines_raw[0][-1, 0], MIN_DRAW_DISTANCE, MAX_DRAW_DISTANCE), True, -0.3)
        road_vertices = []
        max_idx_road_edge = self._get_path_length_idx(lane_lines_raw[0][:, 0], 100.0)
        for i in range(2):
            road_vertices.append(self._map_line_to_polygon(road_edges_raw[i], 0.025, 0.0, max_idx_road_edge, 100.0))
        for i in range(4):
            if lane_vertices[i].size == 0:
                continue
            alpha = 220 if lane_line_probs[i] > 0.3 else 0
            stroke = 0.0
            if i == 1:
                color = rl.Color(*lane_colors.get(left_lane_color, (255, 255, 255)), alpha)
                stroke = 1.0 if left_lane_color in lane_colors else 0.0
            elif i == 2:
                color = rl.Color(*lane_colors.get(right_lane_color, (255, 255, 255)), alpha)
            else:
                color = rl.Color(255, 255, 255, alpha)
            draw_polygon(self._rect, lane_vertices[i], color)
            if stroke > 0.0:
                self._draw_polygon_outline_carrot(lane_vertices[i], color, stroke)
            if i == 1 and left_lane_line % 10 == 4 and (lane_vertices_double.size != 0):
                draw_polygon(self._rect, lane_vertices_double, color)
                if stroke > 0.0:
                    self._draw_polygon_outline_carrot(lane_vertices_double, color, stroke)
        if self._carrot_show_lane_info > 1:
            for i in range(2):
                if road_vertices[i].size == 0:
                    continue
                temp_f = float(np.clip(road_edge_stds[i] / 2.0, 0.0, 1.0))
                color = rl.Color(int((1.0 - temp_f) * 255.0), 0, int(temp_f * 255.0), 255)
                draw_polygon(self._rect, road_vertices[i], color)
